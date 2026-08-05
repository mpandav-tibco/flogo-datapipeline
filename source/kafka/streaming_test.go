package kafka

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// kafkaTestTopic creates a fresh single-partition topic and produces n JSON
// messages to it, returning the topic and group names. Skips when the broker
// env is unset.
func kafkaTestTopic(t *testing.T, n int) (broker, topic, group string) {
	t.Helper()
	broker = os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run the live kafka source test")
	}
	ts := time.Now().UnixNano()
	topic = fmt.Sprintf("dp-src-%d", ts)
	group = fmt.Sprintf("dp-grp-%d", ts)

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

	pcfg := sarama.NewConfig()
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.RequiredAcks = sarama.WaitForAll
	prod, err := sarama.NewSyncProducer([]string{broker}, pcfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i, "name": fmt.Sprintf("row-%d", i)})
		if _, _, err := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	return broker, topic, group
}

// TestKafkaSource_Streaming reads messages from a live broker through the
// streaming-mode consumer (which never returns io.EOF), exercising the
// consumer-group claim, streaming ReadBatch, schema inference and Skip no-op.
func TestKafkaSource_Streaming(t *testing.T) {
	const n = 5
	broker, topic, group := kafkaTestTopic(t, n)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 1000,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	// Skip is a no-op in streaming mode.
	if err := src.Skip(0); err != nil {
		t.Errorf("streaming Skip: %v", err)
	}

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, err := src.ReadBatch(10)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("streamed %d messages, want %d", total, n)
	}
	if sch := src.Schema(); len(sch) != 2 {
		t.Errorf("inferred schema = %v, want 2 columns (id,name)", sch)
	}
}

// TestKafkaSource_BatchDrain reads a bounded topic through batch mode
// (stopAtCurrentEnd), draining to io.EOF — exercising the stop-offset snapshot,
// markPartitionDone and the batch ReadBatch EOF path, plus the batch-mode Skip
// error.
func TestKafkaSource_BatchDrain(t *testing.T) {
	const n = 5
	broker, topic, group := kafkaTestTopic(t, n)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"mode": "batch", "stopAtCurrentEnd": true, "startOffset": "earliest",
		"deserializer": "json", "pollTimeoutMs": 2000,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	// Skip is unsupported in batch mode.
	if err := src.Skip(1); err == nil {
		t.Error("Skip in batch mode should return an error")
	}

	total := 0
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b, err := src.ReadBatch(10)
		if b != nil {
			total += b.Len()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
	}
	if total != n {
		t.Errorf("batch-drained %d messages, want %d", total, n)
	}
}
