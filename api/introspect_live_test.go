package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"
)

func TestIntrospectTablesAndColumnsLive(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live introspect tables/columns test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table := fmt.Sprintf("dp_api_introspect_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE TABLE " + table + " (id INT PRIMARY KEY, label TEXT, amount NUMERIC(10,2))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	s := newTestServerWithPipeline(t)

	w := do(t, s, http.MethodPost, "/v1/introspect/tables", fmt.Sprintf(`{"driver":"postgres","dsn":%q}`, dsn))
	if w.Code != http.StatusOK {
		t.Fatalf("tables = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	wc := do(t, s, http.MethodPost, "/v1/introspect/columns",
		fmt.Sprintf(`{"driver":"postgres","dsn":%q,"table":%q}`, dsn, table))
	if wc.Code != http.StatusOK {
		t.Fatalf("columns = %d, want 200; body=%s", wc.Code, wc.Body.String())
	}
	var res struct {
		Columns []ColumnInfo `json:"columns"`
	}
	if err := json.Unmarshal(wc.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Columns) < 3 {
		t.Errorf("introspect columns = %d, want >= 3", len(res.Columns))
	}
}

func TestIntrospectObjectsLive(t *testing.T) {
	ep := os.Getenv("DP_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT to run the live introspect objects test")
	}
	// Seed a known object under a unique prefix so the assertion is meaningful.
	prefix := fmt.Sprintf("dptest/introspect/%d/", time.Now().UnixNano())
	key := prefix + "seed.txt"
	s3cfg := map[string]interface{}{
		"bucket": "dp-test-bucket", "region": "us-east-1", "endpoint": ep,
		"forcePathStyle": true, "accessKeyId": "minioadmin", "secretAccessKey": "minioadmin",
	}
	store, err := objstore.New("s3", s3cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	sw, err := store.PutWriter(context.Background(), key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := io.WriteString(sw, "hello"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), key) })

	s := newTestServerWithPipeline(t)
	body := fmt.Sprintf(`{"provider":"s3","config":{"bucket":"dp-test-bucket","region":"us-east-1",`+
		`"endpoint":%q,"forcePathStyle":true,"accessKeyId":"minioadmin","secretAccessKey":"minioadmin","prefix":%q}}`, ep, prefix)
	w := do(t, s, http.MethodPost, "/v1/introspect/objects", body)
	if w.Code != http.StatusOK {
		t.Fatalf("objects = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var res struct {
		Objects []struct {
			Key string `json:"key"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, o := range res.Objects {
		if o.Key == key {
			found = true
		}
	}
	if !found {
		t.Errorf("introspect objects did not return the seeded key %q: %+v", key, res.Objects)
	}
}

func TestIntrospectKafkaTopicsLive(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run the live introspect kafka-topics test")
	}
	// Create a known topic so the assertion is meaningful.
	topic := fmt.Sprintf("dp-introspect-%d", time.Now().UnixNano())
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

	s := newTestServerWithPipeline(t)
	w := do(t, s, http.MethodPost, "/v1/introspect/kafka-topics", fmt.Sprintf(`{"brokers":["%s"]}`, broker))
	if w.Code != http.StatusOK {
		t.Fatalf("kafka-topics = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var res struct {
		Topics []string `json:"topics"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, tp := range res.Topics {
		if tp == topic {
			found = true
		}
	}
	if !found {
		t.Errorf("introspect kafka-topics did not return %q: %v", topic, res.Topics)
	}
}
