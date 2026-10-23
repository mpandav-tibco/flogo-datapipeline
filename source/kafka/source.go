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
//	pollTimeoutMs     int       Poll window in milliseconds (alias: pollMs; default 500)
//	deserializer      string    "json" | "avro" | "raw" | "debezium" (default "json")
//	schemaRegistryURL string    Confluent Schema Registry URL (avro only)
//	opColumnName      string    debezium only: metadata column carrying the op (default "__op")
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
//	onDeserializeError string   "skip" (default) logs, counts and skips a message
//	                            that cannot be decoded; "fail" stops the pipeline
//	                            with an error instead of dropping it
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
	// failOnDeserialize stops ReadBatch with an error on an undecodable message
	// (onDeserializeError "fail") instead of skipping it.
	failOnDeserialize bool
	// deserializeErrors counts messages skipped because they could not be decoded.
	deserializeErrors atomic.Int64
	// skippedSinceMark is set when a message was consumed without producing a
	// row (undecodable or HWM-filtered) since the last offset mark, so a poll
	// that returns no rows still records a mark covering it. Guarded by pendingMu.
	skippedSinceMark bool
	// Offset-commit bookkeeping (guarded by pendingMu). The consumer runs on a
	// read-ahead goroutine, so the offset a batch reaches at ReadBatch time is
	// AHEAD of what the pipeline has processed. To avoid committing (and thus
	// losing) un-processed read-ahead messages, each ReadBatch records a mark of
	// (cumulative rows returned, per-partition next-offset); Commit(processedRows)
	// then commits only the offset at the pipeline's processed-row boundary.
	pendingMu    sync.Mutex
	pending      map[int32]int64 // running max next-offset per partition
	returnedRows int64           // cumulative rows returned by ReadBatch this run
	marks        []offMark       // (row, offsets) boundaries, pruned on commit
	commitTarget map[int32]int64 // offsets of the last committed (processed) position
	lastSession  sarama.ConsumerGroupSession
}

// offMark records the per-partition next-offset once a cumulative number of rows
// have been returned by ReadBatch, letting a later Commit map the pipeline's
// processed-row count back to the exact offset to acknowledge.
type offMark struct {
	row     int64
	offsets map[int32]int64
}

// kafkaSourceConfig holds parsed configuration.
type kafkaSourceConfig struct {
	brokers      []string
	topic        string
	groupID      string
	startOffset  int64 // sarama.OffsetNewest, sarama.OffsetOldest, or explicit
	pollMs       int
	deserializer string    // "json" | "avro" | "raw" | "debezium"
	opColumnName string    // debezium only: metadata column carrying the op ("c"|"u"|"d"|"r")
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
		// Guard the send with the session context so a torn-down session (on
		// shutdown, cancelCg → Consume ends the session) unblocks a send that
		// would otherwise block forever on a full msgCh — which would hang Close's
		// wait on cgDone. An abandoned message is simply re-read on the next run.
		select {
		case h.msgCh <- msgEnvelope{session: session, msg: msg}:
		case <-session.Context().Done():
			return nil
		}
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
	onDeserErr, _ := cfg["onDeserializeError"].(string)
	switch onDeserErr {
	case "", "skip", "fail":
	default:
		return nil, fmt.Errorf("kafka source: 'onDeserializeError' must be \"skip\" or \"fail\", got %q", onDeserErr)
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

	// Resume seek: when the engine passes a resume cursor (the per-partition
	// offsets recorded in the checkpoint token, captured atomically with the
	// window-state snapshot), pre-seed the consumer group's committed offsets to
	// exactly those positions BEFORE the group joins. This makes the resumed
	// source position and the restored window state consistent from the same
	// token — independent of whether the last external offset commit succeeded —
	// closing the crash-between-token-and-commit double-count window. Only
	// checkpoint mode injects a cursor, and the token offsets are always >= the
	// last committed offset, so this advances the group without ever rewinding
	// past acknowledged data.
	if commitMode == "checkpoint" {
		resumeOffsets, rerr := parseResumeCursor(cfg["resumeCursor"])
		if rerr != nil {
			return nil, fmt.Errorf("kafka source: %w", rerr)
		}
		if len(resumeOffsets) > 0 {
			if serr := seekGroupOffsets(kc.brokers, kc.groupID, kc.topic, resumeOffsets, sc); serr != nil {
				return nil, fmt.Errorf("kafka source: resume seek: %w", serr)
			}
			klog.Infof("kafka source: resumed group %q to checkpoint offsets %v", kc.groupID, resumeOffsets)
		}
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

		failOnDeserialize: onDeserErr == "fail",
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

// Cursor returns the per-partition offset of the last COMMITTED (processed)
// position as a JSON object keyed by partition. It is captured into the
// checkpoint token atomically with the pipeline state; because it reflects the
// processed boundary (not the read-ahead position), the token's offset and window
// state stay consistent for resume.
func (ks *kafkaSource) Cursor() json.RawMessage {
	ks.pendingMu.Lock()
	defer ks.pendingMu.Unlock()
	m := make(map[string]int64, len(ks.commitTarget))
	for p, o := range ks.commitTarget {
		m[strconv.Itoa(int(p))] = o
	}
	data, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage("null")
	}
	return data
}

// Commit acknowledges consumption up to processedRows — the pipeline's run-local
// processed-row count, which trails the read-ahead position. It maps that count
// to the exact offset via the recorded batch marks and commits only that offset,
// so a crash never loses un-processed read-ahead messages. In "checkpoint" mode
// the engine calls this only after the checkpoint token is durable; in "auto"
// mode sarama owns offsets and this is a no-op.
func (ks *kafkaSource) Commit(processedRows int64) error {
	if ks.commitMode != "checkpoint" {
		return nil // auto mode: sarama auto-commit owns offset progress
	}
	ks.pendingMu.Lock()
	if target, keep := targetForRows(ks.marks, processedRows); target != nil {
		ks.commitTarget = target
		ks.marks = ks.marks[keep:]
	}
	sess := ks.lastSession
	target := ks.commitTarget
	ks.pendingMu.Unlock()
	if sess == nil || target == nil {
		return nil
	}
	// lastSession is the session of the most recent ReadBatch. After a rebalance
	// it may reference an ended session until the next ReadBatch; MarkOffset/Commit
	// on it is then a harmless no-op and the next checkpoint re-commits on the new
	// session. This is sufficient for the single-instance streaming target.
	for p, o := range target {
		sess.MarkOffset(ks.cfg.topic, p, o, "")
	}
	sess.Commit()
	return nil
}

// targetForRows returns the offsets of the newest mark at or before processedRows
// (the pipeline's processed boundary) and how many leading marks can be pruned.
// A nil target means no mark has been reached yet.
func targetForRows(marks []offMark, processedRows int64) (target map[int32]int64, keep int) {
	for i, m := range marks {
		if m.row <= processedRows {
			target = m.offsets
			keep = i + 1
			continue
		}
		break
	}
	return target, keep
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

	// One timer for the whole poll window (the deadline is fixed), instead of
	// allocating a new one per message.
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	for len(batch.Rows) < size {
		var env msgEnvelope
		var ok bool
		select {
		case env, ok = <-ks.handler.msgCh:
			if !ok {
				goto done
			}
		case <-timer.C:
			goto done
		}

		{
			obj, err := deser(env.msg.Key, env.msg.Value)
			if err != nil {
				if ks.failOnDeserialize {
					record.Put(batch)
					return nil, fmt.Errorf("kafka source: deserialize partition=%d offset=%d: %w",
						env.msg.Partition, env.msg.Offset, err)
				}
				n := ks.deserializeErrors.Add(1)
				klog.Warnf("kafka source: skipping undecodable message partition=%d offset=%d (%d skipped so far): %v",
					env.msg.Partition, env.msg.Offset, n, err)
				ks.advancePast(env)
				continue
			}

			ks.mu.RLock()
			schemaReady := ks.schemaSet
			ks.mu.RUnlock()
			if !schemaReady {
				ks.inferSchema(obj)
			}

			if ks.hwmColumn != "" && !ks.passesHWMFilter(obj) {
				ks.advancePast(env)
				continue
			}

			batch.Rows = append(batch.Rows, ks.extractRow(obj))
			ks.pendingMu.Lock()
			if off := env.msg.Offset + 1; off > ks.pending[env.msg.Partition] {
				ks.pending[env.msg.Partition] = off
			}
			ks.returnedRows++
			ks.lastSession = env.session
			ks.pendingMu.Unlock()
		}
	}

done:
	ks.mu.RLock()
	batch.Columns = ks.schema
	ks.mu.RUnlock()

	// Record an offset mark at this batch boundary so a later Commit can map the
	// pipeline's processed-row count to the exact offset to acknowledge. Only in
	// "checkpoint" mode — otherwise Commit never prunes and marks would grow
	// unbounded (auto mode lets sarama own offsets).
	// A poll that only skipped messages also records a mark (at the same row
	// count), so a checkpoint can commit past those messages.
	ks.pendingMu.Lock()
	if ks.commitMode == "checkpoint" && (len(batch.Rows) > 0 || ks.skippedSinceMark) {
		ks.skippedSinceMark = false
		snap := make(map[int32]int64, len(ks.pending))
		for p, o := range ks.pending {
			snap[p] = o
		}
		ks.marks = append(ks.marks, offMark{row: ks.returnedRows, offsets: snap})
	}
	ks.pendingMu.Unlock()

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

// advancePast moves the partition's pending commit offset past a message that
// was consumed but not emitted (skipped as undecodable or filtered by the HWM),
// so a later checkpoint commit covers it instead of redelivering it after every
// restart when it is the last message on its partition.
func (ks *kafkaSource) advancePast(env msgEnvelope) {
	ks.pendingMu.Lock()
	if off := env.msg.Offset + 1; off > ks.pending[env.msg.Partition] {
		ks.pending[env.msg.Partition] = off
	}
	ks.skippedSinceMark = true
	ks.lastSession = env.session
	ks.pendingMu.Unlock()
}

// HasUncommitted implements source.UncommittedReporter: in commitMode
// "checkpoint" it reports consumed offsets beyond the last committed target.
func (ks *kafkaSource) HasUncommitted() bool {
	if ks.commitMode != "checkpoint" {
		return false
	}
	ks.pendingMu.Lock()
	defer ks.pendingMu.Unlock()
	for p, off := range ks.pending {
		if off > ks.commitTarget[p] {
			return true
		}
	}
	return false
}

// DeserializeErrors reports how many messages were skipped as undecodable.
func (ks *kafkaSource) DeserializeErrors() int64 { return ks.deserializeErrors.Load() }

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
	case "debezium":
		opCol := ks.cfg.opColumnName
		return func(key, value []byte) (map[string]interface{}, error) {
			return debeziumDeserialize(opCol, key, value)
		}
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

// debeziumDeserialize decodes a Debezium / Kafka Connect change-event envelope
// — either the "unwrapped" form (schemas.enable=false: the envelope fields are
// top-level) or the "wrapped" form (schemas.enable=true, the default: the
// envelope is nested under a top-level "payload" key alongside a "schema"
// key) — and flattens it to a plain row: the changed record's columns
// ("after" for create/update/snapshot, "before" for a delete, matching
// Debezium's own op semantics) plus a metadata column (opColumn, default
// "__op") carrying the raw op code ("c"|"u"|"d"|"r").
//
// This lets the kafka source consume a real CDC change stream (captured by
// Debezium, Kafka Connect, or anything emitting the same envelope) without
// this engine implementing CDC capture itself — compose with the ecosystem's
// CDC engine rather than duplicate it. Pair this deserializer with the jdbc
// sink's opColumn delete-capable mode (same default column name and the same
// "d"/"delete" vocabulary) to mirror upstream inserts, updates, AND deletes
// into a target table.
//
// A raw Kafka tombstone (a delete's log-compaction marker: an empty value with
// no envelope at all) is NOT decodable here — Debezium's own op="d" envelope
// message already carries the full delete, so the tombstone is redundant for
// this purpose and is reported as an error (skipped, with a warning logged by
// the caller) rather than silently dropped.
func debeziumDeserialize(opColumn string, _, value []byte) (map[string]interface{}, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("debezium: empty value (a raw Kafka tombstone; the preceding op=%q envelope message already carries the delete)", "d")
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(value, &parsed); err != nil {
		return nil, fmt.Errorf("debezium: decode envelope: %w", err)
	}
	envelope := parsed
	if payload, ok := parsed["payload"].(map[string]interface{}); ok {
		envelope = payload // Kafka Connect JSON converter with schemas.enable=true
	}

	op, _ := envelope["op"].(string)
	field := "after"
	if op == "d" {
		field = "before"
	}
	data, _ := envelope[field].(map[string]interface{})
	if data == nil {
		return nil, fmt.Errorf("debezium: envelope has no %q field for op %q", field, op)
	}

	out := make(map[string]interface{}, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	out[opColumn] = op
	return out, nil
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

// parseResumeCursor decodes the checkpoint resume cursor (as produced by
// kafkaSource.Cursor) into per-partition next-offsets. The cursor is a JSON
// object mapping partition id (string) to the next offset to read. It accepts a
// json.RawMessage, []byte, or string; a nil, empty, or "null" cursor yields a
// nil map (no seek).
func parseResumeCursor(v interface{}) (map[int32]int64, error) {
	var raw []byte
	switch t := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		raw = t
	case []byte:
		raw = t
	case string:
		raw = []byte(t)
	default:
		return nil, fmt.Errorf("resumeCursor: unsupported type %T", v)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var m map[string]int64
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("resumeCursor: %w", err)
	}
	out := make(map[int32]int64, len(m))
	for k, off := range m {
		p, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("resumeCursor: bad partition key %q: %w", k, err)
		}
		out[int32(p)] = off
	}
	return out, nil
}

// seekGroupOffsets pre-seeds the consumer group's committed offset for each
// partition to the given next-offset, using a short-lived offset manager, so a
// consumer group created immediately afterward resumes from exactly these
// positions. sarama's MarkOffset only advances the stored offset, and the
// checkpoint-token offsets are always >= the last committed offset, so this
// never rewinds past acknowledged data.
func seekGroupOffsets(brokers []string, group, topic string, offsets map[int32]int64, sc *sarama.Config) error {
	client, err := sarama.NewClient(brokers, sc)
	if err != nil {
		return err
	}
	defer client.Close()

	om, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return err
	}
	defer om.Close()

	poms := make([]sarama.PartitionOffsetManager, 0, len(offsets))
	for p, off := range offsets {
		pom, perr := om.ManagePartition(topic, p)
		if perr != nil {
			for _, done := range poms {
				done.AsyncClose()
			}
			return fmt.Errorf("manage partition %d: %w", p, perr)
		}
		pom.MarkOffset(off, "")
		poms = append(poms, pom)
	}
	// Commit while all partitions are still managed so every marked offset is
	// flushed, then release the per-partition managers.
	om.Commit()
	for _, pom := range poms {
		pom.AsyncClose()
	}
	return nil
}

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

	// Poll window: accept "pollTimeoutMs" (the user-facing key in the docs and
	// examples) with "pollMs" as an alias. Whichever is present and > 0 wins.
	for _, k := range []string{"pollTimeoutMs", "pollMs"} {
		if ms, ok := cfg[k].(int); ok && ms > 0 {
			kc.pollMs = ms
			break
		} else if ms, ok := cfg[k].(float64); ok && ms > 0 {
			kc.pollMs = int(ms)
			break
		}
	}

	if d, ok := cfg["deserializer"].(string); ok && d != "" {
		switch d {
		case "json", "avro", "raw", "debezium":
			kc.deserializer = d
		default:
			return nil, fmt.Errorf("deserializer %q must be 'json', 'avro', 'raw', or 'debezium'", d)
		}
	}
	kc.opColumnName = "__op"
	if oc, ok := cfg["opColumnName"].(string); ok && oc != "" {
		kc.opColumnName = oc
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
