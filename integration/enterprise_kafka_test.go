package integration

// Enterprise scenario: a full Kafka round-trip through the real orchestration —
// Pipeline 1 reads a CSV, maps it, and PUBLISHES JSON messages to a Kafka topic
// (kafka sink); Pipeline 2 BATCH-DRAINS that topic back to a JSONL file (kafka
// source, mode=batch + stopAtCurrentEnd). It proves the kafka sink and the
// bounded-drain source interoperate and preserve fields end-to-end.
//
// Shippable collateral for the "publish to a topic" and "consume a bounded
// topic" patterns. Requires a broker: set KAFKA_TEST_BROKER (e.g. localhost:9092).

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/kafka"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/kafka"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
)

func TestEnterprise_Kafka_RoundTrip(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run the enterprise Kafka round-trip scenario")
	}
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("ent-orders-%d", ts)
	group := fmt.Sprintf("ent-grp-%d", ts)

	// Create a single-partition topic so the batch drain has a definite end-of-log.
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin([]string{broker}, scfg)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(csvPath, []byte("id,customer,amount\n1,Alice,100\n2,Bob,200\n3,Carol,300\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	// --- Pipeline 1: CSV → mapper → Kafka sink (publish 3 JSON messages) ---
	produce := &pipeline.Definition{
		Name:   "csv-to-kafka",
		Source: pipeline.SourceDef{Type: "csv", Path: csvPath, Options: map[string]interface{}{"hasHeader": true}},
		Transforms: []pipeline.TransformDef{
			{Type: "mapper", Extra: map[string]interface{}{
				"type": "mapper",
				"fields": map[string]interface{}{
					"orderId":  "=$id",
					"customer": "=$customer",
					"amount":   "=$amount",
				},
			}},
		},
		Sink: pipeline.SinkDef{Type: "kafka", Options: map[string]interface{}{
			"type": "kafka", "brokers": broker, "topic": topic,
			"serializer": "json", "ackMode": "all",
		}},
	}
	produce.Defaults()
	pOut := runInlinePipeline(t, produce, "produce")
	if pOut.RowsWritten != 3 {
		t.Fatalf("produce written=%d, want 3", pOut.RowsWritten)
	}

	// --- Pipeline 2: Kafka source (bounded batch drain) → JSONL file ---
	outPath := filepath.Join(dir, "consumed.jsonl")
	consume := &pipeline.Definition{
		Name: "kafka-to-jsonl",
		Source: pipeline.SourceDef{Type: "kafka", Options: map[string]interface{}{
			"type": "kafka", "brokers": broker, "topic": topic, "groupId": group,
			"mode": "batch", "stopAtCurrentEnd": true, "deserializer": "json",
			"startOffset": "earliest", "pollTimeoutMs": 2000,
		}},
		Sink: pipeline.SinkDef{Type: "jsonlines", Options: map[string]interface{}{"path": outPath}},
	}
	consume.Defaults()
	cOut := runInlinePipeline(t, consume, "consume")
	if cOut.RowsProcessed != 3 || cOut.RowsWritten != 3 {
		t.Fatalf("consume processed=%d written=%d, want 3/3", cOut.RowsProcessed, cOut.RowsWritten)
	}

	// --- verify fields round-tripped through Kafka ---
	seen := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(readFileString(t, outPath)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse consumed line %q: %v", line, err)
		}
		seen[fmt.Sprintf("%v", m["orderId"])] = fmt.Sprintf("%v", m["customer"])
	}
	if len(seen) != 3 || seen["1"] != "Alice" || seen["2"] != "Bob" || seen["3"] != "Carol" {
		t.Fatalf("round-tripped orders = %v, want {1:Alice,2:Bob,3:Carol}", seen)
	}
}

// runInlinePipeline builds an inline action, runs it, and asserts completion.
func runInlinePipeline(t *testing.T, def *pipeline.Definition, label string) *pipeline.Output {
	t.Helper()
	act, err := pipeline.NewInline(def, "sdk://"+label)
	if err != nil {
		t.Fatalf("%s NewInline: %v", label, err)
	}
	out, err := act.RunDirect(context.Background(), &pipeline.Input{})
	if err != nil {
		t.Fatalf("%s RunDirect: %v", label, err)
	}
	if out.Status != "completed" {
		t.Fatalf("%s status=%q want completed (err=%s)", label, out.Status, out.Error)
	}
	return out
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
