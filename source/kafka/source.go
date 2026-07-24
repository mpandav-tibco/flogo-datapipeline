// Package kafka registers a Kafka consumer source with the source registry.
// It uses the sarama library (github.com/IBM/sarama) — a pure-Go Kafka client
// that avoids CGO and produces fully static binaries.
//
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/kafka"
//
// # Configuration keys
//
//	brokers           []string  Kafka broker addresses (required)
//	topic             string    Kafka topic to consume (required)
//	groupId           string    Consumer group ID (required)
//	startOffset       string    "earliest" | "latest" | int64 string (default "latest")
//	pollMs            int       Poll window in milliseconds (default 500)
//	deserializer      string    "json" | "avro" | "raw" (default "json")
//	schemaRegistryURL string    Confluent Schema Registry URL (avro only)
//	mode              string    "streaming" | "batch" (default "streaming")
//	stopOffset        int64     Batch mode: stop when this offset is reached per partition
//	stopTimestamp     string    Batch mode: stop when message timestamp >= this RFC3339 value
//	stopAtCurrentEnd  bool      Batch mode: drain to each partition's end-of-log snapshotted at open (no sentinel needed)
//	hwmColumn         string    Injected by action.go for incremental pipelines
//	hwmValue          string    The prior watermark value
//	hwmValueType      string    "datetime" | "int" | "string"
//	saslMechanism     string    "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
//	saslUser          string    SASL username
//	saslPasswordEnv   string    Env var name holding the SASL password
//	tlsCertFile       string    Path to TLS client certificate file
//	tlsKeyFile        string    Path to TLS client key file
//	tlsCACertFile     string    Path to TLS CA certificate file
package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/kafkautil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

var klog = corelog.ChildLogger(corelog.RootLogger(), "kafka-source")

func init() {
	source.Register("kafka", newKafkaSource)
}

// kafkaSource implements source.Source backed by a sarama ConsumerGroup.
//
// Architecture:
//
//	ConsumerGroup goroutine (sarama-managed):
//	  ConsumeClaim → enqueues messages into msgCh
//
//	Pipeline goroutine (ReadBatch caller):
//	  drains msgCh with a timed poll (pollMs milliseconds)
//
// The buffered channel (depth = batchSize*2) decouples Kafka rebalance events
// from the pipeline read loop. Offset marks are set inside ConsumeClaim;
// sarama's auto-commit flushes them to Kafka every second.
type kafkaSource struct {
	cfg      *kafkaSourceConfig
	cg       sarama.ConsumerGroup
	handler  *consumerGroupHandler
	cancelCg context.CancelFunc
	cgDone   chan struct{} // closed when the CG goroutine exits

	// schema is inferred from the first message (json/avro) or fixed (raw).
	mu        sync.RWMutex
	schema    []record.Column
	schemaSet bool

	closed atomic.Bool

	// HWM filter — populated from action.go injected keys.
	hwmColumn    string
	hwmValue     string
	hwmValueType string

	schemaRegistry *schemaRegistryClient // non-nil only for avro deserializer

	// commitMode: "auto" (sarama auto-commit, default) or "checkpoint" (offsets
	// committed only when the pipeline checkpoints, via Commit()).
	commitMode string
	// pending is the next offset to commit per partition — the position of
	// messages already RETURNED by ReadBatch (hence processed by checkpoint time).
	// Cursor snapshots it into the checkpoint token; Commit acks it to Kafka.
	pendingMu   sync.Mutex
	pending     map[int32]int64
	lastSession sarama.ConsumerGroupSession
}

// kafkaSourceConfig holds parsed configuration.
type kafkaSourceConfig struct {
	brokers      []string
	topic        string
	groupID      string
	startOffset  int64 // sarama.OffsetNewest, sarama.OffsetOldest, or explicit
	pollMs       int
	deserializer string    // "json" | "avro" | "raw"
	mode         string    // "streaming" | "batch"
	stopOffset   int64     // batch mode: stop at this offset (0 = not set)
	stopTS       time.Time // batch mode: stop at or after this timestamp (zero = not set)
	stopAtEnd    bool      // batch mode: stop when each partition's end-of-log (HWM at open) is reached
}

// msgEnvelope wraps a Kafka message with the session for offset marking.
type msgEnvelope struct {
	session sarama.ConsumerGroupSession
	msg     *sarama.ConsumerMessage
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	msgCh     chan msgEnvelope
	ready     chan struct{} // closed when the first ConsumeClaim starts
	readyOnce sync.Once

	batchMode  bool
	autoMark   bool // mark offsets on enqueue (auto commitMode); false = commit at checkpoint
	stopOffset int64
	stopTS     time.Time

	// stopAtEnd + endOffsets support "drain to current end of log": endOffsets
	// holds the per-partition high-water-mark snapshotted at source open, so a
	// batch run finishes exactly when every assigned partition has been read up
	// to that boundary (no sentinel message required).
	stopAtEnd  bool
	endOffsets map[int32]int64
	partMu     sync.Mutex
	donePart   map[int32]bool

	reachedStop atomic.Int32
}

// markPartitionDone records that a partition has reached its stop boundary and
// flips reachedStop once every partition with a known end offset is complete.
func (h *consumerGroupHandler) markPartitionDone(partition int32) {
	h.partMu.Lock()
	defer h.partMu.Unlock()
	if h.donePart == nil {
		h.donePart = make(map[int32]bool)
	}
	h.donePart[partition] = true
	if len(h.donePart) >= len(h.endOffsets) {
		h.reachedStop.Store(1)
	}
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim is called by sarama in a dedicated goroutine per partition.
// It enqueues messages into h.msgCh (blocking on backpressure) and marks
// each offset so sarama's auto-commit can flush it to Kafka.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	h.readyOnce.Do(func() { close(h.ready) })

	// Drain-to-end: if this partition has no messages past where we start
	// reading, it is already complete.
	if h.batchMode && h.stopAtEnd {
		end, known := h.endOffsets[claim.Partition()]
		if !known || claim.InitialOffset() >= end || end <= 0 {
			h.markPartitionDone(claim.Partition())
			return nil
		}
	}

	for msg := range claim.Messages() {
		if h.batchMode && h.stopOffset > 0 && msg.Offset >= h.stopOffset {
			h.reachedStop.Store(1)
			return nil
		}
		if h.batchMode && !h.stopTS.IsZero() && !msg.Timestamp.Before(h.stopTS) {
			h.reachedStop.Store(1)
			return nil
		}
		h.msgCh <- msgEnvelope{session: session, msg: msg}
		if h.autoMark {
			session.MarkMessage(msg, "")
		}

		// Drain-to-end: this partition is complete once we've consumed the last
		// message that existed at source-open time (end offset - 1).
		if h.batchMode && h.stopAtEnd {
			if end, known := h.endOffsets[msg.Partition]; known && msg.Offset >= end-1 {
				h.markPartitionDone(msg.Partition)
				return nil
			}
		}
	}
	return nil
}

func newKafkaSource(cfg map[string]interface{}) (source.Source, error) {
	kc, err := parseSourceConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka source: %w", err)
	}

	sc, err := kafkautil.BuildSaramaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka source: %w", err)
	}

	commitMode, _ := cfg["commitMode"].(string)
	if commitMode == "" {
		commitMode = "auto"
	}
	sc.Consumer.Offsets.Initial = kc.startOffset
	sc.Consumer.Offsets.AutoCommit.Enable = commitMode != "checkpoint"
	sc.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	batchSize := 1000
	if bs, ok := cfg["batchSize"].(int); ok && bs > 0 {
		batchSize = bs
	} else if bs, ok := cfg["batchSize"].(float64); ok && bs > 0 {
		batchSize = int(bs)
	}

	handler := &consumerGroupHandler{
		msgCh:      make(chan msgEnvelope, batchSize*2),
		ready:      make(chan struct{}),
		batchMode:  kc.mode == "batch",
		autoMark:   commitMode != "checkpoint",
		stopOffset: kc.stopOffset,
		stopTS:     kc.stopTS,
		stopAtEnd:  kc.stopAtEnd,
	}

	// Drain-to-end: snapshot each partition's high-water-mark (end of log) now,
	// before consuming, so the batch finishes at the current end regardless of
	// records produced after the run starts. Uses a short-lived client.
	if handler.batchMode && kc.stopAtEnd {
		endOffsets, oerr := snapshotEndOffsets(kc.brokers, kc.topic, sc)
		if oerr != nil {
			return nil, fmt.Errorf("kafka source: snapshot end offsets: %w", oerr)
		}
		handler.endOffsets = endOffsets
	}

	cg, err := sarama.NewConsumerGroup(kc.brokers, kc.groupID, sc)
	if err != nil {
		return nil, fmt.Errorf("kafka source: new consumer group: %w", err)
	}

	cgCtx, cgCancel := context.WithCancel(context.Background())
	cgDone := make(chan struct{})

	go func() {
		defer close(cgDone)
		for {
			if cgCtx.Err() != nil {
				return
			}
			if err := cg.Consume(cgCtx, []string{kc.topic}, handler); err != nil {
				if cgCtx.Err() != nil {
					return
				}
				klog.Errorf("kafka source: consumer group error (will retry): %v", err)
			}
		}
	}()

	// Wait for at least one partition claim.
	select {
	case <-handler.ready:
	case <-time.After(30 * time.Second):
		cgCancel()
		_ = cg.Close()
		return nil, fmt.Errorf("kafka source: timed out waiting for consumer group partition assignment")
	}

	ks := &kafkaSource{
		cfg:        kc,
		cg:         cg,
		handler:    handler,
		cancelCg:   cgCancel,
		cgDone:     cgDone,
		commitMode: commitMode,
		pending:    make(map[int32]int64),
	}

	ks.hwmColumn, _ = cfg["hwmColumn"].(string)
	ks.hwmValue, _ = cfg["hwmValue"].(string)
	ks.hwmValueType, _ = cfg["hwmValueType"].(string)

	if kc.deserializer == "avro" {
		srURL, _ := cfg["schemaRegistryURL"].(string)
		if srURL == "" {
			cgCancel()
			_ = cg.Close()
			return nil, fmt.Errorf("kafka source: schemaRegistryURL is required when deserializer is 'avro'")
		}
		ks.schemaRegistry = newSchemaRegistryClient(srURL)
	}

	if kc.deserializer == "raw" {
		ks.mu.Lock()
		ks.schema = []record.Column{
			{Name: "key", Type: record.TypeString},
			{Name: "value", Type: record.TypeString},
		}
		ks.schemaSet = true
		ks.mu.Unlock()
	}

	return ks, nil
}

// Schema returns the column schema. Spins until the first message is
// deserialized (for json/avro) or returns immediately (for raw).
func (ks *kafkaSource) Schema() []record.Column {
	for {
		ks.mu.RLock()
		set, schema := ks.schemaSet, ks.schema
		ks.mu.RUnlock()
		if set {
			return schema
		}
		if ks.closed.Load() {
			return schema
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Skip is a no-op in streaming mode. Returns an error in batch mode because
// Kafka offset resumption is handled via MarkMessage+auto-commit, not row counts.
func (ks *kafkaSource) Skip(n int64) error {
	if ks.cfg.mode == "batch" {
		return fmt.Errorf("kafka source: Skip not supported in batch mode — offset resumption uses consumer group commits")
	}
	return nil
}

// Cursor returns the per-partition next-offset position of messages already
// returned by ReadBatch, as a JSON object keyed by partition. It is captured
// into the checkpoint token atomically with the pipeline state, so a resume can
// see exactly where consumption had reached at the checkpoint.
func (ks *kafkaSource) Cursor() json.RawMessage {
	ks.pendingMu.Lock()
	defer ks.pendingMu.Unlock()
	m := make(map[string]int64, len(ks.pending))
	for p, o := range ks.pending {
		m[strconv.Itoa(int(p))] = o
	}
	data, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage("null")
	}
	return data
}

// Commit acknowledges consumption up to the last-returned offset per partition.
// In "checkpoint" commitMode the engine calls this only after the checkpoint
// token is durable, so committed offsets never run ahead of persisted progress
// (no lost messages on resume). In "auto" mode sarama also auto-commits; this
// call simply makes the checkpoint boundary an explicit commit point too.
func (ks *kafkaSource) Commit() error {
	if ks.commitMode != "checkpoint" {
		return nil // auto mode: sarama auto-commit owns offset progress
	}
	ks.pendingMu.Lock()
	sess := ks.lastSession
	snapshot := make(map[int32]int64, len(ks.pending))
	for p, o := range ks.pending {
		snapshot[p] = o
	}
	ks.pendingMu.Unlock()
	if sess == nil {
		return nil
	}
	for p, o := range snapshot {
		sess.MarkOffset(ks.cfg.topic, p, o, "")
	}
	sess.Commit()
	return nil
}

// ReadBatch polls the consumer channel for up to pollMs milliseconds and
// returns up to size messages as a RecordBatch.
//
// Streaming mode: an empty batch (not io.EOF) is returned when the poll
// window closes with no messages — the pipeline loop stays alive.
//
// Batch mode: (partial-or-full-batch, io.EOF) is returned when the stop
// condition is reached.
func (ks *kafkaSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if ks.closed.Load() {
		return nil, io.EOF
	}

	deadline := time.Now().Add(time.Duration(ks.cfg.pollMs) * time.Millisecond)
	deser := ks.chooseDeserializer()

	batch := record.Get()
	if cap(batch.Rows) >= size {
		batch.Rows = batch.Rows[:0]
	} else {
		batch.Rows = make([][]interface{}, 0, size)
	}

	for len(batch.Rows) < size {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		timer := time.NewTimer(remaining)
		var env msgEnvelope
		var ok bool
		select {
		case env, ok = <-ks.handler.msgCh:
			timer.Stop()
			if !ok {
				goto done
			}
		case <-timer.C:
			goto done
		}

		{
			obj, err := deser(env.msg.Key, env.msg.Value)
			if err != nil {
				klog.Warnf("kafka source: deserialize partition=%d offset=%d: %v",
					env.msg.Partition, env.msg.Offset, err)
				continue
			}

			ks.mu.RLock()
			schemaReady := ks.schemaSet
			ks.mu.RUnlock()
			if !schemaReady {
				ks.inferSchema(obj)
			}

			if ks.hwmColumn != "" && !ks.passesHWMFilter(obj) {
				continue
			}

			batch.Rows = append(batch.Rows, ks.extractRow(obj))
			ks.pendingMu.Lock()
			if off := env.msg.Offset + 1; off > ks.pending[env.msg.Partition] {
				ks.pending[env.msg.Partition] = off
			}
			ks.lastSession = env.session
			ks.pendingMu.Unlock()
		}
	}

done:
	ks.mu.RLock()
	batch.Columns = ks.schema
	ks.mu.RUnlock()

	// Batch mode: only signal end-of-stream once the stop boundary is reached
	// AND the channel is fully drained, so no buffered messages are lost when
	// more than one batch's worth were queued before the stop fired.
	if ks.cfg.mode == "batch" && ks.handler.reachedStop.Load() == 1 && len(ks.handler.msgCh) == 0 {
		if len(batch.Rows) == 0 {
			record.Put(batch)
			return nil, io.EOF
		}
		return batch, io.EOF
	}
	return batch, nil
}

// Flush ensures committed offsets are persisted to Kafka.
// sarama auto-commit runs every second; sleeping slightly longer guarantees
// the marks set by recent ConsumeClaim calls are flushed before the pipeline
// writes its checkpoint token.
func (ks *kafkaSource) Flush() error {
	time.Sleep(1100 * time.Millisecond)
	return nil
}

// Close cancels the consumer group, drains the message channel, and waits
// for the goroutine to exit cleanly.
func (ks *kafkaSource) Close() error {
	if ks.closed.Swap(true) {
		return nil
	}
	ks.cancelCg()
	// Wait for consumer group goroutine to finish, then close msgCh so the
	// drain goroutine below can exit. Closing before cgDone would race with
	// ConsumeClaim senders that are still running.
	<-ks.cgDone
	close(ks.handler.msgCh)
	// Drain any messages buffered before close so ConsumeClaim senders don't block.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for range ks.handler.msgCh {
		}
	}()
	<-drainDone
	return ks.cg.Close()
}

func (ks *kafkaSource) inferSchema(obj map[string]interface{}) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	// Insertion-sort for deterministic column ordering.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	cols := make([]record.Column, len(keys))
	for i, k := range keys {
		cols[i] = record.Column{Name: k, Type: record.TypeAny}
	}
	ks.mu.Lock()
	ks.schema = cols
	ks.schemaSet = true
	ks.mu.Unlock()
}

func (ks *kafkaSource) extractRow(obj map[string]interface{}) []interface{} {
	ks.mu.RLock()
	schema := ks.schema
	ks.mu.RUnlock()
	row := make([]interface{}, len(schema))
	for i, col := range schema {
		row[i] = obj[col.Name]
	}
	return row
}

// passesHWMFilter returns true when the message's hwmColumn value is strictly
// greater than the stored hwmValue. Mirrors the coercion logic in hwm/transform.go.
func (ks *kafkaSource) passesHWMFilter(obj map[string]interface{}) bool {
	raw, ok := obj[ks.hwmColumn]
	if !ok {
		return true // column absent — pass through
	}
	switch ks.hwmValueType {
	case "int":
		return hwmCoerceInt64(raw) > hwmCoerceInt64(ks.hwmValue)
	case "datetime":
		mt := hwmCoerceDatetime(raw)
		ht := hwmCoerceDatetime(ks.hwmValue)
		if mt != nil && ht != nil {
			return mt.(time.Time).After(ht.(time.Time))
		}
		return mt != nil
	default: // "string"
		return fmt.Sprintf("%v", raw) > ks.hwmValue
	}
}

func (ks *kafkaSource) chooseDeserializer() func(key, value []byte) (map[string]interface{}, error) {
	switch ks.cfg.deserializer {
	case "avro":
		sr := ks.schemaRegistry
		return func(key, value []byte) (map[string]interface{}, error) {
			return avroDeserialize(sr, key, value)
		}
	case "raw":
		return rawDeserialize
	default:
		return jsonDeserialize
	}
}

// --- Deserializers ---

func jsonDeserialize(_, value []byte) (map[string]interface{}, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal(value, &obj); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}
	return obj, nil
}

func rawDeserialize(key, value []byte) (map[string]interface{}, error) {
	return map[string]interface{}{
		"key":   string(key),
		"value": string(value),
	}, nil
}

// avroDeserialize decodes a Confluent wire-format Avro message.
// Wire format: 0x00 magic byte + 4-byte big-endian schema ID + Avro binary payload.
func avroDeserialize(sr *schemaRegistryClient, _, value []byte) (map[string]interface{}, error) {
	if len(value) < 5 {
		return nil, fmt.Errorf("avro: message too short (%d bytes, need at least 5)", len(value))
	}
	if value[0] != 0x00 {
		return nil, fmt.Errorf("avro: invalid Confluent wire format — expected magic byte 0x00, got 0x%02x", value[0])
	}
	schemaID := int(binary.BigEndian.Uint32(value[1:5]))
	schemaJSON, err := sr.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("avro: fetch schema id=%d: %w", schemaID, err)
	}
	return decodeAvroRecord(schemaJSON, value[5:])
}

// --- Schema Registry ---

type schemaRegistryClient struct {
	url   string
	mu    sync.RWMutex
	cache map[int]string
}

func newSchemaRegistryClient(url string) *schemaRegistryClient {
	return &schemaRegistryClient{url: url, cache: make(map[int]string)}
}

func (c *schemaRegistryClient) GetSchema(id int) (string, error) {
	c.mu.RLock()
	if s, ok := c.cache[id]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	url := fmt.Sprintf("%s/schemas/ids/%d", c.url, id)
	resp, err := http.Get(url) //nolint:gosec // URL comes from trusted operator config
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode schema registry response: %w", err)
	}

	c.mu.Lock()
	c.cache[id] = result.Schema
	c.mu.Unlock()
	return result.Schema, nil
}

// --- Avro binary decoder ---

// decodeAvroRecord decodes an Avro binary payload using the provided schema JSON.
// Handles top-level record types with primitive and nullable (two-element union) fields.
func decodeAvroRecord(schemaJSON string, payload []byte) (map[string]interface{}, error) {
	var schema struct {
		Type   string `json:"type"`
		Fields []struct {
			Name string      `json:"name"`
			Type interface{} `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("parse Avro schema: %w", err)
	}
	if schema.Type != "record" {
		return nil, fmt.Errorf("top-level Avro type must be 'record', got %q", schema.Type)
	}

	obj := make(map[string]interface{}, len(schema.Fields))
	r := &avroBinaryReader{data: payload}

	for _, field := range schema.Fields {
		nullable, typeName := avroUnwrapUnion(field.Type)
		if nullable {
			idx, err := r.readLong()
			if err != nil {
				return nil, fmt.Errorf("field %q: read union index: %w", field.Name, err)
			}
			if idx == 0 {
				obj[field.Name] = nil
				continue
			}
			// idx == 1: read the non-null branch.
		}
		val, err := r.readType(typeName)
		if err != nil {
			return nil, fmt.Errorf("field %q (type=%q): %w", field.Name, typeName, err)
		}
		obj[field.Name] = val
	}
	return obj, nil
}

// avroUnwrapUnion returns (isNullable, nonNullTypeName) for an Avro type descriptor.
func avroUnwrapUnion(t interface{}) (nullable bool, typeName string) {
	switch v := t.(type) {
	case string:
		return false, v
	case []interface{}:
		for _, elem := range v {
			if s, ok := elem.(string); ok {
				if s == "null" {
					nullable = true
				} else if typeName == "" {
					typeName = s
				}
			}
		}
	}
	if typeName == "" {
		typeName = "string"
	}
	return
}

// avroBinaryReader is a minimal streaming Avro binary decoder.
type avroBinaryReader struct {
	data []byte
	pos  int
}

func (r *avroBinaryReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *avroBinaryReader) readLong() (int64, error) {
	var n uint64
	var shift uint
	for {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}
		n |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return int64((n >> 1) ^ -(n & 1)), nil // zig-zag decode
}

func (r *avroBinaryReader) readBytes() ([]byte, error) {
	length, err := r.readLong()
	if err != nil {
		return nil, err
	}
	if length < 0 || r.pos+int(length) > len(r.data) {
		return nil, fmt.Errorf("avro: invalid bytes length %d at pos %d", length, r.pos)
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

func (r *avroBinaryReader) readString() (string, error) {
	b, err := r.readBytes()
	return string(b), err
}

func (r *avroBinaryReader) readFloat() (float32, error) {
	if r.pos+4 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	bits := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return math.Float32frombits(bits), nil
}

func (r *avroBinaryReader) readDouble() (float64, error) {
	if r.pos+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	bits := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return math.Float64frombits(bits), nil
}

func (r *avroBinaryReader) readBoolean() (bool, error) {
	b, err := r.readByte()
	return b != 0, err
}

func (r *avroBinaryReader) readType(typeName string) (interface{}, error) {
	switch typeName {
	case "string":
		return r.readString()
	case "bytes":
		return r.readBytes()
	case "int":
		v, err := r.readLong()
		return int64(int32(v)), err // Avro int is 32-bit
	case "long":
		return r.readLong()
	case "float":
		return r.readFloat()
	case "double":
		return r.readDouble()
	case "boolean":
		return r.readBoolean()
	case "null":
		return nil, nil
	default:
		klog.Warnf("kafka source: avro unsupported type %q — reading as string", typeName)
		return r.readString()
	}
}

// --- Config parser ---

// snapshotEndOffsets returns the current end-of-log (high-water-mark) offset for
// every partition of a topic, using a short-lived client. Called at source open
// so a batch "drain to end" run finishes at a stable boundary regardless of
// records produced after it starts.
func snapshotEndOffsets(brokers []string, topic string, sc *sarama.Config) (map[int32]int64, error) {
	client, err := sarama.NewClient(brokers, sc)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("list partitions for %q: %w", topic, err)
	}
	ends := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		end, err := client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("get end offset partition=%d: %w", p, err)
		}
		ends[p] = end
	}
	return ends, nil
}

func parseSourceConfig(cfg map[string]interface{}) (*kafkaSourceConfig, error) {
	kc := &kafkaSourceConfig{
		startOffset:  sarama.OffsetNewest,
		pollMs:       500,
		deserializer: "json",
		mode:         "streaming",
	}

	switch v := cfg["brokers"].(type) {
	case []string:
		kc.brokers = v
	case []interface{}:
		for _, b := range v {
			if s, ok := b.(string); ok {
				kc.brokers = append(kc.brokers, s)
			}
		}
	case string:
		kc.brokers = []string{v}
	}
	if len(kc.brokers) == 0 {
		return nil, fmt.Errorf("'brokers' is required")
	}

	kc.topic, _ = cfg["topic"].(string)
	if kc.topic == "" {
		return nil, fmt.Errorf("'topic' is required")
	}

	kc.groupID, _ = cfg["groupId"].(string)
	if kc.groupID == "" {
		return nil, fmt.Errorf("'groupId' is required")
	}

	switch v := cfg["startOffset"].(type) {
	case string:
		switch v {
		case "earliest":
			kc.startOffset = sarama.OffsetOldest
		case "latest", "":
			kc.startOffset = sarama.OffsetNewest
		default:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("startOffset %q is not 'earliest', 'latest', or a valid int64", v)
			}
			kc.startOffset = n
		}
	case int64:
		kc.startOffset = v
	case float64:
		kc.startOffset = int64(v)
	case int:
		kc.startOffset = int64(v)
	}

	if ms, ok := cfg["pollMs"].(int); ok && ms > 0 {
		kc.pollMs = ms
	} else if ms, ok := cfg["pollMs"].(float64); ok && ms > 0 {
		kc.pollMs = int(ms)
	}

	if d, ok := cfg["deserializer"].(string); ok && d != "" {
		switch d {
		case "json", "avro", "raw":
			kc.deserializer = d
		default:
			return nil, fmt.Errorf("deserializer %q must be 'json', 'avro', or 'raw'", d)
		}
	}

	if m, ok := cfg["mode"].(string); ok && m != "" {
		switch m {
		case "streaming", "batch":
			kc.mode = m
		default:
			return nil, fmt.Errorf("mode %q must be 'streaming' or 'batch'", m)
		}
	}

	if so, ok := cfg["stopOffset"].(int64); ok {
		kc.stopOffset = so
	} else if so, ok := cfg["stopOffset"].(float64); ok {
		kc.stopOffset = int64(so)
	}

	if sts, ok := cfg["stopTimestamp"].(string); ok && sts != "" {
		t, err := time.Parse(time.RFC3339, sts)
		if err != nil {
			return nil, fmt.Errorf("stopTimestamp %q: %w", sts, err)
		}
		kc.stopTS = t.UTC()
	}

	if b, ok := cfg["stopAtCurrentEnd"].(bool); ok {
		kc.stopAtEnd = b
	}

	return kc, nil
}

// --- HWM coercion helpers (mirrors hwm/transform.go) ---

func hwmCoerceInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func hwmCoerceDatetime(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case string:
		for _, layout := range []string{
			time.RFC3339Nano, time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.UTC()
			}
		}
	}
	return nil
}
