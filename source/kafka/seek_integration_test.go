package kafka

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// TestSeekGroupOffsetsLive verifies that a checkpoint-mode source with a resume
// cursor pre-seeds the consumer group's committed offset and resumes from
// exactly that position. It requires a broker: set KAFKA_TEST_BROKER (e.g.
// localhost:9092). It produces 5 messages to a fresh topic, opens a source that
// seeks to offset 3, and asserts only the last 2 messages are delivered —
// proving the resume seek repositions the group regardless of startOffset.
func TestSeekGroupOffsetsLive(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test (e.g. localhost:9092)")
	}
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("seek-test-%d", ts)
	group := fmt.Sprintf("seek-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	defer func() { _ = admin.DeleteTopic(topic) }()

	prod, err := sarama.NewSyncProducer([]string{broker}, pc)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{
			Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("%d", i)),
		}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	// Seek to offset 3: only offsets 3 and 4 should remain to read.
	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"commitMode": "checkpoint", "deserializer": "raw", "mode": "streaming",
		"startOffset": "earliest", "pollTimeoutMs": 500,
		"resumeCursor": []byte(`{"0":3}`),
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	total := 0
	empties := 0
	for empties < 4 && total < 10 {
		b, rerr := src.ReadBatch(10)
		if b != nil {
			total += b.Len()
		}
		if rerr != nil && rerr != io.EOF {
			t.Fatalf("read: %v", rerr)
		}
		if b == nil || b.Len() == 0 {
			empties++
		} else {
			empties = 0
		}
	}
	if total != 2 {
		t.Fatalf("seek to offset 3 of 5 → want 2 messages delivered, got %d", total)
	}
}
