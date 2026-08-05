package integration

// Enterprise scenario: a real CDC change stream (Debezium/Kafka-Connect-shaped
// envelopes, produced directly onto the topic — NOT via the kafka sink, so the
// envelope shape is fully controlled) consumed by the kafka source's debezium
// deserializer and mirrored — inserts, updates, AND deletes — into a live
// Postgres table via the jdbc sink's opColumn delete-capable mode.
//
// This is an adversarial test: the message stream deliberately includes a
// malformed envelope, a raw tombstone, both the "unwrapped" and "wrapped"
// (schemas.enable=true) envelope forms, AND — the interesting case — TWO
// changes to the SAME key (a create then an update) with nothing in between
// to force an intermediate flush, which is exactly the shape a real CDC
// stream produces when a row changes twice in quick succession.
//
// Requires a live broker (KAFKA_TEST_BROKER) and Postgres (DP_TEST_PG_DSN).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/kafka"
)

func TestEnterprise_CDC_KafkaDebeziumToJDBC(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	pgDSN := os.Getenv("DP_TEST_PG_DSN")
	if broker == "" || pgDSN == "" {
		t.Skip("set KAFKA_TEST_BROKER and DP_TEST_PG_DSN to run the enterprise CDC scenario")
	}

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	table := fmt.Sprintf("ent_cdc_customers_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	// --- produce a realistic, adversarial CDC message stream ---
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("ent-cdc-%d", ts)
	group := fmt.Sprintf("ent-cdc-grp-%d", ts)

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

	send := func(value []byte) {
		t.Helper()
		if _, _, err := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value)}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// 1. Snapshot read (op=r), unwrapped envelope — id=1.
	send([]byte(`{"before":null,"after":{"id":1,"name":"Alice","status":"open"},"op":"r"}`))
	// 2. Create (op=c), WRAPPED envelope (schemas.enable=true) — id=2.
	send([]byte(`{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":2,"name":"Bob","status":"open"},"op":"c"}}`))
	// 3. Adversarial: a SECOND change to id=1 with NOTHING in between to force
	//    an intermediate flush — the realistic "row changed twice quickly"
	//    shape a live CDC stream produces, and the exact pattern that can
	//    trigger Postgres's "ON CONFLICT DO UPDATE ... row a second time" if
	//    the sink naively bundles both changes into one upsert statement.
	send([]byte(`{"before":{"id":1,"name":"Alice","status":"open"},"after":{"id":1,"name":"Alice","status":"vip"},"op":"u"}`))
	// 4. Malformed envelope — must be skipped, not crash the run.
	send([]byte(`{not-json`))
	// 5. Raw Kafka tombstone (log-compaction delete marker, no envelope at
	//    all) — must be skipped, not crash the run.
	send(nil)
	// 6. Delete (op=d) — id=2. "before" carries the deleted row's key.
	send([]byte(`{"before":{"id":2,"name":"Bob","status":"open"},"after":null,"op":"d"}`))
	// 7. Create (op=c), wrapped — id=3.
	send([]byte(`{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":3,"name":"Carol","status":"open"},"op":"c"}}`))

	def := &pipeline.Definition{
		Name: "enterprise-cdc-kafka-to-jdbc",
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

	act, err := pipeline.NewInline(def, "sdk://enterprise-cdc")
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
	// 7 messages sent; 2 (malformed + tombstone) must be skipped by the
	// deserializer, so only 5 valid CDC envelopes should have been processed.
	if out.RowsProcessed != 5 {
		t.Errorf("RowsProcessed = %d, want 5 (7 sent - 1 malformed - 1 tombstone)", out.RowsProcessed)
	}

	rows, err := db.Query("SELECT id, name, status FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	type gotRow struct {
		id           int64
		name, status string
	}
	var got []gotRow
	for rows.Next() {
		var r gotRow
		if err := rows.Scan(&r.id, &r.name, &r.status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	// id=1: created (open) then updated (vip) -> final status "vip".
	// id=2: created then deleted -> must be ABSENT.
	// id=3: created -> present, status "open".
	want := []gotRow{
		{1, "Alice", "vip"},
		{3, "Carol", "open"},
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %+v, want %+v (id=2 must have been deleted)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// The metadata op column must never leak into the target table.
	var opColCount int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = '__op'", table,
	).Scan(&opColCount); err != nil {
		t.Fatalf("check __op column: %v", err)
	}
	if opColCount != 0 {
		t.Error("__op leaked into the target table as a real column")
	}
}
