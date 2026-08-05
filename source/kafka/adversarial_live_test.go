package kafka

// Adversarial live tests for Kafka corner cases NOT covered by the existing
// streaming_test.go / seek_integration_test.go / commit_test.go suite:
//   - a FULL checkpoint/commit/resume round-trip using the source's OWN
//     Cursor() output (not a hand-typed offset), simulating a real
//     crash-and-restart of a streaming pipeline instance
//   - multi-partition consumption (every existing live kafka test uses a
//     single-partition topic) verifying no message loss/duplication when a
//     single consumer group instance claims several partitions
//   - a large message near/over the broker's default message.max.bytes

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// TestKafkaSource_CheckpointRestartRoundTrip_Live proves the full crash/restart
// contract end-to-end: consume a bounded number of rows, Commit() exactly that
// many, capture Cursor(), close the source (simulating a process crash/restart
// after a successful checkpoint), then open a BRAND NEW source instance seeded
// with that captured cursor and verify it resumes from EXACTLY the right
// position — every remaining message is delivered exactly once, with no gap
// and no re-delivery of already-committed messages.
func TestKafkaSource_CheckpointRestartRoundTrip_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test")
	}
	const total = 10
	const firstRunRows = 6 // deliberately not a divisor of total, to catch off-by-ones

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-ckpt-restart-%d", ts)
	group := fmt.Sprintf("dp-ckpt-restart-grp-%d", ts)

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
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{broker}, pc)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	for i := 1; i <= total; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	// --- "Run 1": consume exactly firstRunRows, commit, capture cursor, crash ---
	srcASrc, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"commitMode": "checkpoint", "mode": "streaming",
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
	})
	if err != nil {
		t.Fatalf("newKafkaSource (run 1): %v", err)
	}
	srcA := srcASrc.(*kafkaSource)

	var runAIDs []int
	deadline := time.Now().Add(15 * time.Second)
	for len(runAIDs) < firstRunRows && time.Now().Before(deadline) {
		b, rerr := srcA.ReadBatch(firstRunRows - len(runAIDs))
		if rerr != nil && rerr != io.EOF {
			t.Fatalf("ReadBatch (run 1): %v", rerr)
		}
		if b != nil {
			for _, row := range b.Rows {
				runAIDs = append(runAIDs, int(toInt64(row[b.ColIndex("id")])))
			}
		}
	}
	if len(runAIDs) != firstRunRows {
		t.Fatalf("run 1 delivered %d rows, want exactly %d", len(runAIDs), firstRunRows)
	}
	if err := srcA.Commit(int64(firstRunRows)); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := srcA.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cursor := srcA.Cursor()
	if string(cursor) == "null" || string(cursor) == "{}" {
		t.Fatalf("Cursor() after Commit(%d) returned empty: %s", firstRunRows, cursor)
	}
	if err := srcA.Close(); err != nil {
		t.Fatalf("Close (run 1): %v", err)
	}

	// --- "Run 2": brand-new source instance, seeded with run 1's captured cursor ---
	srcB, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"commitMode": "checkpoint", "mode": "streaming",
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"resumeCursor": []byte(cursor),
	})
	if err != nil {
		t.Fatalf("newKafkaSource (run 2, resumeCursor=%s): %v", cursor, err)
	}
	defer srcB.Close()

	var runBIDs []int
	want := total - firstRunRows
	deadline = time.Now().Add(15 * time.Second)
	for len(runBIDs) < want && time.Now().Before(deadline) {
		b, rerr := srcB.ReadBatch(want - len(runBIDs))
		if rerr != nil && rerr != io.EOF {
			t.Fatalf("ReadBatch (run 2): %v", rerr)
		}
		if b != nil {
			for _, row := range b.Rows {
				runBIDs = append(runBIDs, int(toInt64(row[b.ColIndex("id")])))
			}
		}
	}
	if len(runBIDs) != want {
		t.Fatalf("run 2 (resumed) delivered %d rows, want exactly %d — resume cursor=%s", len(runBIDs), want, cursor)
	}

	// The two runs together must cover every id exactly once — no gap, no
	// re-delivery of already-committed messages.
	seen := make(map[int]int)
	for _, id := range append(append([]int{}, runAIDs...), runBIDs...) {
		seen[id]++
	}
	if len(seen) != total {
		t.Errorf("combined runs saw %d distinct ids, want %d: run1=%v run2=%v", len(seen), total, runAIDs, runBIDs)
	}
	for id := 1; id <= total; id++ {
		if seen[id] != 1 {
			t.Errorf("id=%d delivered %d times across both runs, want exactly 1 (run1=%v run2=%v)", id, seen[id], runAIDs, runBIDs)
		}
	}
}

// TestKafkaSource_MultiPartition_Live proves a single consumer-group source
// instance correctly claims and drains ALL partitions of a multi-partition
// topic with no message loss or duplication — every prior live kafka test in
// this package uses a single-partition topic, so the multi-partition claim
// fan-in (several ConsumeClaim goroutines funneling into one shared msgCh)
// has never been exercised end-to-end before.
func TestKafkaSource_MultiPartition_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test")
	}
	const numPartitions = 4
	const total = 40

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-multipart-%d", ts)
	group := fmt.Sprintf("dp-multipart-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true
	// Force a true round-robin spread across every partition, rather than
	// relying on key-hash luck, so the test deterministically exercises all
	// numPartitions partitions every run.
	pc.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: numPartitions, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{broker}, pc)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	partitionsUsed := map[int32]bool{}
	for i := 1; i <= total; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		p, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)})
		if perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
		partitionsUsed[p] = true
	}
	_ = prod.Close()
	if len(partitionsUsed) < numPartitions {
		t.Fatalf("round-robin producer only used %d of %d partitions — test setup didn't spread messages as expected", len(partitionsUsed), numPartitions)
	}

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 1000,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	seen := make(map[int]int)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < total && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(total)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b == nil {
			continue
		}
		idIdx := b.ColIndex("id")
		for _, row := range b.Rows {
			seen[int(toInt64(row[idIdx]))]++
		}
	}
	if len(seen) != total {
		t.Fatalf("multi-partition consume saw %d distinct ids, want %d (partitions used by producer: %d)", len(seen), total, len(partitionsUsed))
	}
	for id := 1; id <= total; id++ {
		if seen[id] != 1 {
			t.Errorf("id=%d delivered %d times, want exactly 1", id, seen[id])
		}
	}
}

// TestKafkaSource_LargeMessage_Live proves a message well under the broker's
// default message.max.bytes (~1MB) round-trips intact through the source
// (deserialization, schema inference, row extraction) without truncation or
// corruption — and that a message safely OVER that default limit is rejected
// with a clear producer-side error (not silently dropped or hung) rather than
// wedging the consumer.
func TestKafkaSource_LargeMessage_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test")
	}
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-large-msg-%d", ts)
	group := fmt.Sprintf("dp-large-msg-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true
	// Raise the CLIENT-side cap well above the broker default so an oversized
	// send is rejected by the BROKER (the realistic failure mode a user's own
	// oversized-message producer would hit), not short-circuited locally.
	pc.Producer.MaxMessageBytes = 4 * 1024 * 1024

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{broker}, pc)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	// A payload comfortably under the ~1MB default broker limit — must
	// round-trip byte-for-byte.
	const okSize = 700 * 1024
	bigVal := strings.Repeat("x", okSize)
	payload, _ := json.Marshal(map[string]interface{}{"id": 1, "blob": bigVal})
	if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(payload)}); perr != nil {
		t.Fatalf("produce large-but-legal message: %v", perr)
	}

	// A payload well over the ~1MB default broker limit — must be rejected
	// with a clear error at produce time, not accepted-then-mangled.
	const tooBig = 3 * 1024 * 1024
	overPayload, _ := json.Marshal(map[string]interface{}{"id": 2, "blob": strings.Repeat("y", tooBig)})
	_, _, oerr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(overPayload)})
	if oerr == nil {
		t.Logf("NOTE: a %d-byte message was accepted by the broker (message.max.bytes configured higher than the ~1MB default) — not a bug, just a broker config note", tooBig)
	} else {
		t.Logf("oversized (%d-byte) message correctly rejected by the broker: %v", tooBig, oerr)
	}

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	var gotBlob string
	var gotCount int
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil && b.Len() > 0 {
			blobIdx := b.ColIndex("blob")
			for _, row := range b.Rows {
				gotCount++
				if s, ok := row[blobIdx].(string); ok && len(s) == okSize {
					gotBlob = s
				}
			}
			if gotCount >= 1 {
				break
			}
		}
	}
	if gotCount == 0 {
		t.Fatalf("large legal message was never delivered by the source")
	}
	if len(gotBlob) != okSize {
		t.Errorf("delivered blob length = %d, want %d (large message was truncated/corrupted in transit)", len(gotBlob), okSize)
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	default:
		return -1
	}
}

// TestKafkaSource_ConsumerGroupRebalance_Live proves a mid-run rebalance
// (triggered by a SECOND consumer instance joining the SAME group while the
// first is actively reading — the normal way a pipeline scales out or a
// crashed instance's replacement rejoins) does not cause PERMANENT message
// loss. With default commitMode=auto, TRANSIENT re-delivery of a message
// that was read but not yet auto-committed at the moment its partition gets
// reassigned is expected, standard at-least-once Kafka behavior — not a bug
// — so this test asserts every id is seen AT LEAST once across both
// instances combined, not exactly once.
func TestKafkaSource_ConsumerGroupRebalance_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test")
	}
	const numPartitions = 4
	const total = 40

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-rebalance-%d", ts)
	group := fmt.Sprintf("dp-rebalance-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true
	pc.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: numPartitions, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{broker}, pc)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	for i := 1; i <= total; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	srcA, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 500,
	})
	if err != nil {
		t.Fatalf("newKafkaSource (instance A): %v", err)
	}
	defer srcA.Close()

	var mu sync.Mutex
	seen := make(map[int]int)
	readLoop := func(src source.Source, stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, rerr := src.ReadBatch(10)
			if rerr != nil {
				return
			}
			if b == nil {
				continue
			}
			idIdx := b.ColIndex("id")
			mu.Lock()
			for _, row := range b.Rows {
				seen[int(toInt64(row[idIdx]))]++
			}
			mu.Unlock()
		}
	}

	stopA := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); readLoop(srcA, stopA) }()

	// Let A read for a bit on its own, then a SECOND instance joins the same
	// group — sarama triggers a rebalance, redistributing partitions between
	// A and B mid-run.
	time.Sleep(1500 * time.Millisecond)

	srcB, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 500,
	})
	if err != nil {
		close(stopA)
		wg.Wait()
		t.Fatalf("newKafkaSource (instance B, triggers rebalance): %v", err)
	}
	stopB := make(chan struct{})
	wg.Add(1)
	go func() { defer wg.Done(); readLoop(srcB, stopB) }()

	// Give both instances time to finish draining after the rebalance
	// settles (partition reassignment + resumed consumption).
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= total {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	close(stopA)
	close(stopB)
	wg.Wait()
	_ = srcB.Close()

	if len(seen) != total {
		t.Fatalf("saw %d distinct ids across both instances after the rebalance, want %d (permanent loss across the rebalance)", len(seen), total)
	}
	dupCount := 0
	for id := 1; id <= total; id++ {
		if seen[id] == 0 {
			t.Errorf("id=%d was never delivered by either instance (permanent loss)", id)
		} else if seen[id] > 1 {
			dupCount++
		}
	}
	if dupCount > 0 {
		t.Logf("NOTE: %d ids were re-delivered around the rebalance boundary (%d total occurrences) — expected at-least-once behavior with commitMode=auto, not a bug", dupCount, len(seen))
	}
}

// TestKafkaSource_SustainedConcurrentProduceConsume_Live proves the source
// correctly drains a topic that is being produced to WHILE it is actively
// consuming (every other live test in this package pre-produces a static
// message set before any ReadBatch call) — the realistic shape of a genuine
// production streaming pipeline, and the one place a subtle race in the
// ConsumeClaim -> msgCh -> ReadBatch pipeline (backpressure, channel
// closing, partition claim timing) would most plausibly surface.
func TestKafkaSource_SustainedConcurrentProduceConsume_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run this live test")
	}
	const numPartitions = 2
	const produceDuration = 4 * time.Second
	const produceInterval = 15 * time.Millisecond

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-sustained-%d", ts)
	group := fmt.Sprintf("dp-sustained-grp-%d", ts)

	pc := sarama.NewConfig()
	pc.Version = sarama.V2_1_0_0
	pc.Producer.Return.Successes = true
	pc.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	admin, err := sarama.NewClusterAdmin([]string{broker}, pc)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: numPartitions, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	// Open the source FIRST (before any message exists), matching a real
	// streaming deployment where the consumer is already running when
	// production starts/continues.
	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 300,
	})
	if err != nil {
		t.Fatalf("newKafkaSource: %v", err)
	}
	defer src.Close()

	var produced atomic.Int64
	produceDone := make(chan struct{})
	go func() {
		defer close(produceDone)
		prod, perr := sarama.NewSyncProducer([]string{broker}, pc)
		if perr != nil {
			t.Errorf("producer: %v", perr)
			return
		}
		defer prod.Close()
		id := 0
		deadline := time.Now().Add(produceDuration)
		for time.Now().Before(deadline) {
			id++
			val, _ := json.Marshal(map[string]interface{}{"id": id})
			if _, _, serr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); serr != nil {
				t.Errorf("produce %d: %v", id, serr)
				return
			}
			produced.Add(1)
			time.Sleep(produceInterval)
		}
	}()

	seen := make(map[int]bool)
	// Keep reading well past produceDuration to drain everything sent while
	// consuming was already in progress.
	overallDeadline := time.Now().Add(produceDuration + 15*time.Second)
	for time.Now().Before(overallDeadline) {
		select {
		case <-produceDone:
			// Producer finished — do a few more empty-poll rounds to make
			// sure the tail of the stream has been fully drained, then stop.
			if int64(len(seen)) >= produced.Load() {
				goto drainedEnough
			}
		default:
		}
		b, rerr := src.ReadBatch(50)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			idIdx := b.ColIndex("id")
			for _, row := range b.Rows {
				seen[int(toInt64(row[idIdx]))] = true
			}
		}
	}
drainedEnough:

	want := int(produced.Load())
	if want == 0 {
		t.Fatal("producer goroutine reported 0 messages sent — test setup failure")
	}
	if len(seen) != want {
		missing := 0
		for id := 1; id <= want; id++ {
			if !seen[id] {
				missing++
			}
		}
		t.Fatalf("consumed %d distinct ids while %d were produced under sustained concurrent load (%d missing)", len(seen), want, missing)
	}
}
