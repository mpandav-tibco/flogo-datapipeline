package integration

// Multi-partition CDC scenario: unlike enterprise_cdc_test.go (a single
// partition, used there to isolate the same-key-twice-in-one-flush bug), this
// test uses a FOUR-partition topic with the producer KEYED by entity id — the
// same setup Debezium uses in production (partition = hash(key)) so that every
// change to a given row lands on the same partition (preserving per-entity
// order) while DIFFERENT entities are spread across partitions and consumed
// concurrently by the kafka source's multiple ConsumeClaim goroutines funneling
// into one shared channel. This exercises a combination no other test covers:
// multi-partition consumption + the Debezium deserializer + the jdbc sink's
// per-flush conflictKey dedup, across 5 independent entities with different
// create/update/delete sequences (including a delete-then-recreate).
//
// Requires a live broker (KAFKA_TEST_BROKER) and Postgres (DP_TEST_PG_DSN).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/kafka"
)

func TestEnterprise_CDC_MultiPartition_KafkaDebeziumToJDBC(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	pgDSN := os.Getenv("DP_TEST_PG_DSN")
	if broker == "" || pgDSN == "" {
		t.Skip("set KAFKA_TEST_BROKER and DP_TEST_PG_DSN to run the multi-partition enterprise CDC scenario")
	}

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	table := fmt.Sprintf("ent_cdc_mp_customers_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("ent-cdc-mp-%d", ts)
	group := fmt.Sprintf("ent-cdc-mp-grp-%d", ts)

	const numPartitions = 4
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin([]string{broker}, scfg)
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: numPartitions, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	pcfg := sarama.NewConfig()
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.RequiredAcks = sarama.WaitForAll
	// Default hash partitioner + an explicit Key: messages with the same key
	// always land on the same partition (Debezium's own partitioning scheme),
	// so per-entity ordering is preserved even though entities are spread
	// across all numPartitions partitions.
	prod, err := sarama.NewSyncProducer([]string{broker}, pcfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	partitionsSeen := map[int32]bool{}
	send := func(key string, value string) {
		t.Helper()
		p, _, err := prod.SendMessage(&sarama.ProducerMessage{
			Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(value),
		})
		if err != nil {
			t.Fatalf("send key=%s: %v", key, err)
		}
		partitionsSeen[p] = true
	}

	// Five entities, interleaved (as a real concurrent multi-entity stream
	// would be), each with a distinct create/update/delete sequence:
	//   id=1: create(open) -> update(vip)                          => final vip
	//   id=2: create(open) -> delete                                => final absent
	//   id=3: create(open)                                          => final open (untouched)
	//   id=4: create(pending) -> update(active) -> update(closed)   => final closed
	//   id=5: create(open) -> delete -> create(open) (re-created)   => final open, present again
	send("1", `{"before":null,"after":{"id":1,"name":"E1","status":"open"},"op":"c"}`)
	send("4", `{"before":null,"after":{"id":4,"name":"E4","status":"pending"},"op":"c"}`)
	send("2", `{"before":null,"after":{"id":2,"name":"E2","status":"open"},"op":"c"}`)
	send("5", `{"before":null,"after":{"id":5,"name":"E5","status":"open"},"op":"c"}`)
	send("3", `{"before":null,"after":{"id":3,"name":"E3","status":"open"},"op":"c"}`)
	send("1", `{"before":{"id":1,"name":"E1","status":"open"},"after":{"id":1,"name":"E1","status":"vip"},"op":"u"}`)
	send("4", `{"before":{"id":4,"name":"E4","status":"pending"},"after":{"id":4,"name":"E4","status":"active"},"op":"u"}`)
	send("5", `{"before":{"id":5,"name":"E5","status":"open"},"after":null,"op":"d"}`)
	send("2", `{"before":{"id":2,"name":"E2","status":"open"},"after":null,"op":"d"}`)
	send("4", `{"before":{"id":4,"name":"E4","status":"active"},"after":{"id":4,"name":"E4","status":"closed"},"op":"u"}`)
	send("5", `{"before":null,"after":{"id":5,"name":"E5","status":"open"},"op":"c"}`)

	if len(partitionsSeen) < 2 {
		t.Fatalf("test setup didn't spread keys across multiple partitions (only %d used) — hash-partitioner assumption broken", len(partitionsSeen))
	}

	def := &pipeline.Definition{
		Name: "enterprise-cdc-mp-kafka-to-jdbc",
		Source: pipeline.SourceDef{
			Type: "kafka",
			Options: map[string]interface{}{
				"type": "kafka", "brokers": broker, "topic": topic, "groupId": group,
				"mode": "batch", "stopAtCurrentEnd": true, "deserializer": "debezium",
				"startOffset": "earliest", "pollTimeoutMs": 3000,
			},
		},
		Sink: pipeline.SinkDef{
			Type: "jdbc",
			Options: map[string]interface{}{
				"type": "jdbc", "dsn": pgDSN, "driver": "postgres", "table": table,
				"autoMigrate": true, "onConflict": "upsert",
				"conflictKey": []interface{}{"id"}, "opColumn": "__op",
			},
		},
	}
	def.Defaults()

	act, err := pipeline.NewInline(def, "sdk://enterprise-cdc-mp")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	out, err := act.RunDirect(context.Background(), &pipeline.Input{})
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q want completed (error=%s, processed=%d written=%d failed=%d)",
			out.Status, out.Error, out.RowsProcessed, out.RowsWritten, out.RowsFailed)
	}
	if out.RowsProcessed != 11 {
		t.Errorf("RowsProcessed = %d, want 11 (all messages are well-formed)", out.RowsProcessed)
	}
	// RowsDeleted must be 1, not 2: the stream has two delete ops (id=5, id=2),
	// but id=5's delete is immediately superseded by a recreate later in the
	// SAME flush (batchSize=5000 default, so all 11 messages land in one
	// flush) — writeBatchWithOp's removeStalePendingDelete cancels id=5's
	// pending delete out before any SQL runs (see sink/jdbc/sink.go), so only
	// id=2's delete ever reaches the database as a real DELETE statement.
	// This is the multipartition counterpart of the delete-then-recreate
	// ordering fix verified by TestJDBCSink_Live_DeleteThenRecreateSameKey_AllDialects.
	if out.RowsDeleted != 1 {
		t.Errorf("RowsDeleted = %d, want 1 (only id=2's delete reaches the DB; id=5's delete is cancelled by its same-flush recreate)", out.RowsDeleted)
	}

	rows, err := db.Query("SELECT id, name, status FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	got := map[int64]string{}
	for rows.Next() {
		var id int64
		var name, status string
		if err := rows.Scan(&id, &name, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = status
	}

	want := map[int64]string{1: "vip", 3: "open", 4: "closed", 5: "open"} // id=2 must be absent
	if len(got) != len(want) {
		t.Fatalf("final rows = %v, want %v (id=2 must have been deleted)", got, want)
	}
	for id, status := range want {
		if got[id] != status {
			t.Errorf("id=%s status = %q, want %q", strconv.FormatInt(id, 10), got[id], status)
		}
	}
	if _, present := got[2]; present {
		t.Errorf("id=2 must be deleted (final DELETE was the last op), but a row is still present: status=%q", got[2])
	}
}
