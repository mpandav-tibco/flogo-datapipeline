package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/kafkautil"
)

// handleIntrospectKafkaTopics handles POST /v1/introspect/kafka-topics.
// Body: { "brokers": ["localhost:9092"], "saslMechanism": "...", "saslUser": "...",
//
//	"saslPasswordEnv": "...", "tlsCACertFile": "...", ... }
//
// Returns { "topics": ["orders", "events", ...] } — user topics only (internal
// topics prefixed with "_" or "__" are hidden).
func (s *Server) handleIntrospectKafkaTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Brokers []string               `json:"brokers"`
		Config  map[string]interface{} `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	// Allow brokers to be nested under config too (Studio sends the whole stage config).
	brokers := body.Brokers
	if len(brokers) == 0 && body.Config != nil {
		brokers = stringSlice(body.Config["brokers"])
	}
	if len(brokers) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "brokers are required")
		return
	}
	cfg := body.Config
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	topics, err := introspectKafkaTopics(brokers, cfg)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"topics": topics})
}

// introspectKafkaTopics lists non-internal topic names from a Kafka cluster.
func introspectKafkaTopics(brokers []string, cfg map[string]interface{}) ([]string, error) {
	sc, err := kafkautil.BuildSaramaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka config: %w", err)
	}
	// Introspection is a one-off, interactive Studio call (unlike the kafka
	// source/sink's long-lived connections, which legitimately want
	// BuildSaramaConfig's shared defaults) — it should fail fast rather than
	// block for sarama's default Net.DialTimeout (30s) if a broker address is
	// wrong or unreachable. sc.Admin.Timeout already defaults to a reasonable
	// 3s (sarama's own default), so only the dial timeout needs tightening.
	sc.Net.DialTimeout = 8 * time.Second
	admin, err := sarama.NewClusterAdmin(brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	defer admin.Close()

	detail, err := admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	out := make([]string, 0, len(detail))
	for name := range detail {
		if strings.HasPrefix(name, "_") {
			continue // internal topics (__consumer_offsets, _schemas, ...)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// handleIntrospectSchemaRegistry handles POST /v1/introspect/schema-registry.
// Body: { "url": "http://localhost:8081", "subject": "orders-value" }
//
//	or  { "url": "...", "topic": "orders" }  (subject defaults to "<topic>-value")
//
// Fetches the latest registered Avro/JSON schema and maps it to pipeline columns.
func (s *Server) handleIntrospectSchemaRegistry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		URL     string `json:"url"`
		Subject string `json:"subject"`
		Topic   string `json:"topic"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.URL == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "schema registry url is required")
		return
	}
	subject := body.Subject
	if subject == "" && body.Topic != "" {
		subject = body.Topic + "-value"
	}
	if subject == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "subject or topic is required")
		return
	}
	cols, err := introspectSchemaRegistry(body.URL, subject)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"columns": cols, "subject": subject})
}

// introspectSchemaRegistry fetches the latest schema for a subject from a
// Confluent-compatible schema registry and maps the top-level fields to
// pipeline columns. Supports Avro (default) and JSON schema types.
func introspectSchemaRegistry(baseURL, subject string) ([]ColumnInfo, error) {
	url := strings.TrimRight(baseURL, "/") + "/subjects/" + subject + "/versions/latest"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var reg struct {
		Subject    string `json:"subject"`
		Version    int    `json:"version"`
		SchemaType string `json:"schemaType"` // "" or "AVRO" => Avro; "JSON"; "PROTOBUF"
		Schema     string `json:"schema"`
	}
	if err := json.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("parse registry response: %w", err)
	}
	switch strings.ToUpper(reg.SchemaType) {
	case "", "AVRO":
		return avroSchemaColumns(reg.Schema)
	case "JSON":
		return jsonSchemaColumns(reg.Schema)
	default:
		return nil, fmt.Errorf("unsupported schema type %q (only AVRO and JSON are supported)", reg.SchemaType)
	}
}

// avroSchemaColumns maps the top-level fields of an Avro record schema to columns.
func avroSchemaColumns(schemaJSON string) ([]ColumnInfo, error) {
	var s struct {
		Type   string `json:"type"`
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &s); err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}
	if s.Type != "record" || len(s.Fields) == 0 {
		return nil, fmt.Errorf("avro schema is not a record with fields")
	}
	out := make([]ColumnInfo, 0, len(s.Fields))
	for _, f := range s.Fields {
		avroType, nullable := avroFieldType(f.Type)
		out = append(out, ColumnInfo{
			Name:     f.Name,
			Type:     mapAvroType(avroType),
			DBType:   avroType,
			Nullable: nullable,
		})
	}
	return out, nil
}

// avroFieldType resolves an Avro field's type, unwrapping union types like
// ["null", "string"] (which mark a nullable field) and named/logical types.
func avroFieldType(raw json.RawMessage) (typ string, nullable bool) {
	// Simple string type: "string", "int", ...
	var str string
	if json.Unmarshal(raw, &str) == nil {
		return str, false
	}
	// Union: ["null", "string"] or ["string", "null"].
	var union []json.RawMessage
	if json.Unmarshal(raw, &union) == nil {
		for _, u := range union {
			var us string
			if json.Unmarshal(u, &us) == nil && us == "null" {
				nullable = true
				continue
			}
			t, _ := avroFieldType(u)
			if t != "" {
				typ = t
			}
		}
		return typ, nullable
	}
	// Complex/logical type: { "type": "long", "logicalType": "timestamp-millis" } or record/array/map.
	var obj struct {
		Type        string `json:"type"`
		LogicalType string `json:"logicalType"`
	}
	if json.Unmarshal(raw, &obj) == nil {
		if obj.LogicalType != "" {
			return obj.LogicalType, nullable
		}
		return obj.Type, nullable
	}
	return "", nullable
}

// mapAvroType maps an Avro/logical type name to a pipeline column type.
func mapAvroType(t string) string {
	switch strings.ToLower(t) {
	case "int", "long":
		return "int"
	case "float", "double":
		return "float"
	case "boolean":
		return "bool"
	case "bytes", "decimal":
		return "decimal"
	case "date", "timestamp-millis", "timestamp-micros", "time-millis", "time-micros", "local-timestamp-millis":
		return "date"
	case "string", "enum", "fixed":
		return "string"
	default:
		return "string"
	}
}

// jsonSchemaColumns maps the top-level properties of a JSON schema to columns.
func jsonSchemaColumns(schemaJSON string) ([]ColumnInfo, error) {
	var s struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Type   json.RawMessage `json:"type"`
			Format string          `json:"format"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &s); err != nil {
		return nil, fmt.Errorf("parse json schema: %w", err)
	}
	if len(s.Properties) == 0 {
		return nil, fmt.Errorf("json schema has no properties")
	}
	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}
	names := make([]string, 0, len(s.Properties))
	for name := range s.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ColumnInfo, 0, len(names))
	for _, name := range names {
		p := s.Properties[name]
		jt, nullable := jsonSchemaType(p.Type)
		pt := mapJSONSchemaType(jt, p.Format)
		out = append(out, ColumnInfo{
			Name:     name,
			Type:     pt,
			DBType:   jt,
			Nullable: nullable || !required[name],
		})
	}
	return out, nil
}

// jsonSchemaType resolves a JSON schema "type" that may be a string or an array
// like ["string", "null"].
func jsonSchemaType(raw json.RawMessage) (typ string, nullable bool) {
	var str string
	if json.Unmarshal(raw, &str) == nil {
		return str, false
	}
	var arr []string
	if json.Unmarshal(raw, &arr) == nil {
		for _, t := range arr {
			if t == "null" {
				nullable = true
				continue
			}
			typ = t
		}
	}
	return typ, nullable
}

// mapJSONSchemaType maps a JSON schema type + format to a pipeline column type.
func mapJSONSchemaType(t, format string) string {
	switch strings.ToLower(format) {
	case "date", "date-time", "time":
		return "date"
	}
	switch strings.ToLower(t) {
	case "integer":
		return "int"
	case "number":
		return "float"
	case "boolean":
		return "bool"
	default:
		return "string"
	}
}

// stringSlice coerces an interface{} (typically []interface{} from JSON) into a
// []string, ignoring non-string elements.
func stringSlice(v interface{}) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if t == "" {
			return nil
		}
		// Allow comma-separated broker lists.
		parts := strings.Split(t, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	return nil
}
