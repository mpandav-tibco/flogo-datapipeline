package kafka

// TestKafkaSource_BrokerRestartResilience_Live is deliberately NOT part of the
// normal test-integration-up flow: unlike every other live test in this
// package, it actually RESTARTS the shared Kafka broker container mid-run via
// `docker restart`, which would disrupt any other test (in this package or
// concurrently-running packages, e.g. sink/jdbc or integration) relying on the
// same broker at that moment. It only runs when BOTH KAFKA_TEST_BROKER and
// KAFKA_TEST_BROKER_CONTAINER (the docker container name, e.g.
// flogo-datapipeline-kafka-1) are explicitly set, and is meant to be run in
// ISOLATION:
//
//	KAFKA_TEST_BROKER=localhost:9092 KAFKA_TEST_BROKER_CONTAINER=flogo-datapipeline-kafka-1 \
//	  go test -run TestKafkaSource_BrokerRestartResilience_Live ./source/kafka/...
//
// It proves the source recovers automatically from a broker restart (a
// realistic operational event — a rolling upgrade, a container OOM-kill and
// restart, a brief network partition) without the pipeline operator having to
// restart the whole process: sarama's ConsumerGroup goroutine already retries
// Consume() in a loop on error (see the goroutine started in newKafkaSource),
// so this test is really verifying that retry loop + our msgCh/ReadBatch
// plumbing behaves correctly across a REAL broker outage, not simulating one.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestKafkaSource_BrokerRestartResilience_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	container := os.Getenv("KAFKA_TEST_BROKER_CONTAINER")
	if broker == "" || container == "" {
		t.Skip("set KAFKA_TEST_BROKER and KAFKA_TEST_BROKER_CONTAINER (e.g. flogo-datapipeline-kafka-1) to run this live broker-restart resilience test — it actually restarts the shared broker, so run it in ISOLATION, not as part of a full-suite run")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available on PATH — cannot restart the broker container")
	}

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-broker-restart-%d", ts)
	group := fmt.Sprintf("dp-broker-restart-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		admin.Close()
		t.Fatalf("create topic: %v", err)
	}
	admin.Close()
	t.Cleanup(func() {
		if a, err := sarama.NewClusterAdmin([]string{broker}, pc); err == nil {
			_ = a.DeleteTopic(topic)
			a.Close()
		}
	})

	produce := func(from, to int) {
		t.Helper()
		prod, perr := sarama.NewSyncProducer([]string{broker}, pc)
		if perr != nil {
			t.Fatalf("producer: %v", perr)
		}
		defer prod.Close()
		for i := from; i <= to; i++ {
			val, _ := json.Marshal(map[string]interface{}{"id": i})
			if _, _, serr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); serr != nil {
				t.Fatalf("produce %d: %v", i, serr)
			}
		}
	}

	const beforeCount = 5
	const afterCount = 5
	const total = beforeCount + afterCount

	produce(1, beforeCount)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 500,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	seen := make(map[int]bool)
	drainUntil := func(want int, deadline time.Time) {
		t.Helper()
		for len(seen) < want && time.Now().Before(deadline) {
			b, rerr := src.ReadBatch(20)
			if rerr != nil {
				t.Fatalf("ReadBatch: %v", rerr)
			}
			if b == nil {
				continue
			}
			idIdx := b.ColIndex("id")
			for _, row := range b.Rows {
				seen[int(toInt64(row[idIdx]))] = true
			}
		}
	}

	// Drain the pre-restart batch first, so we know the source was genuinely
	// connected and consuming BEFORE the broker goes down.
	drainUntil(beforeCount, time.Now().Add(15*time.Second))
	if len(seen) != beforeCount {
		t.Fatalf("before restart: consumed %d of %d messages — source wasn't reliably connected before the restart, test setup problem", len(seen), beforeCount)
	}
	t.Logf("consumed all %d pre-restart messages; restarting broker container %q now", beforeCount, container)

	if out, err := exec.Command("docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v (output: %s)", container, err, out)
	}

	// Give the broker a little time to actually go down before we start
	// polling — otherwise a fast restart could complete before ReadBatch even
	// notices the connection dropped.
	time.Sleep(2 * time.Second)

	// Produce the second batch WHILE the broker is (probably still) coming
	// back up — retried internally by sarama's producer/admin machinery —
	// proving the whole pipeline (not just the consumer) recovers.
	produce(beforeCount+1, total)

	// The source must recover on its own (no restart of the source itself)
	// and eventually deliver every message, pre- and post-restart alike.
	// Broker restart + Raft/controller re-election can take longer than a
	// normal test timeout, hence the generous deadline.
	drainUntil(total, time.Now().Add(90*time.Second))

	if len(seen) != total {
		missing := []int{}
		for id := 1; id <= total; id++ {
			if !seen[id] {
				missing = append(missing, id)
			}
		}
		t.Fatalf("after broker restart: consumed %d of %d total messages, missing=%v — the source did not recover from the broker restart", len(seen), total, missing)
	}
	t.Logf("source recovered from the broker restart and delivered all %d messages (%d before + %d after)", total, beforeCount, afterCount)
}
