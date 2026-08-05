package kafka

// Live SASL/TLS coverage — the last honestly-noted Kafka corner-case gap.
// docker-compose.test.yml's kafka service exposes three additional listeners
// (see the compose file's comment for why the listener names are chosen the
// way they are, working around a Kafka Docker image entrypoint quirk):
//   - MTLS (:9094)          mutual TLS, no SASL, ssl.client.auth=required
//   - CLIENTSASL (:9095)    SASL/PLAIN + SASL/SCRAM-SHA-256/512 over plaintext, no TLS
//   - CLIENTSASLTLS (:9096) SASL_SSL — SASL/PLAIN auth over an encrypted (TLS)
//     transport, ssl.client.auth=none (SASL authenticates the client, TLS
//     just encrypts + authenticates the server) — the standard production
//     SASL_SSL shape, distinct from both plain SASL and mutual TLS alone.
//
// Run scripts/gen-kafka-test-certs.sh once to generate the gitignored
// .kafka-certs/ directory before running these tests, and set:
//
//	KAFKA_TEST_MTLS_BROKER=localhost:9094
//	KAFKA_TEST_SASL_BROKER=localhost:9095
//	KAFKA_TEST_SASLSSL_BROKER=localhost:9096
//	KAFKA_TEST_SCRAM_USER / KAFKA_TEST_SCRAM_PASSWORD           (SCRAM-SHA-256; make test-integration-up default: testuser256/testpass256)
//	KAFKA_TEST_SCRAM512_USER / KAFKA_TEST_SCRAM512_PASSWORD     (SCRAM-SHA-512; make test-integration-up default: testuser512/testpass512)
//	KAFKA_TEST_CERTS_DIR=<repo-root>/.kafka-certs   (defaults to ../../.kafka-certs)
//
// Each is skipped independently when its broker/credential env vars are unset.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func certsDir(t *testing.T) string {
	t.Helper()
	if d := os.Getenv("KAFKA_TEST_CERTS_DIR"); d != "" {
		return d
	}
	return filepath.Join("..", "..", ".kafka-certs")
}

// mtlsSaramaConfig builds a sarama config for direct (test-side) producer/
// admin use against the MTLS listener, for topic setup / message production —
// mirroring what kafkautil.BuildSaramaConfig does for the source under test.
func mtlsSaramaConfig(t *testing.T) *sarama.Config {
	t.Helper()
	dir := certsDir(t)
	cert, err := loadClientCert(dir)
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}
	pool, err := loadCAPool(dir)
	if err != nil {
		t.Fatalf("load CA pool: %v", err)
	}
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_6_0_0
	sc.Producer.Return.Successes = true
	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = tlsConfigFor(cert, pool)
	return sc
}

// TestKafkaSource_MutualTLS_Live proves the source connects, authenticates via
// mutual TLS (presenting tlsCertFile/tlsKeyFile, verifying the broker via
// tlsCACertFile), and streams messages correctly end-to-end.
func TestKafkaSource_MutualTLS_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_MTLS_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_MTLS_BROKER (e.g. localhost:9094) to run this live mutual-TLS test")
	}
	dir := certsDir(t)
	for _, f := range []string{"ca.crt", "client.crt", "client.key"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Skipf("missing %s in %s — run scripts/gen-kafka-test-certs.sh first: %v", f, dir, err)
		}
	}

	const n = 5
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-mtls-%d", ts)
	group := fmt.Sprintf("dp-mtls-grp-%d", ts)

	scfg := mtlsSaramaConfig(t)
	admin, err := sarama.NewClusterAdmin([]string{broker}, scfg)
	if err != nil {
		t.Fatalf("cluster admin (mTLS): %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{broker}, scfg)
	if err != nil {
		t.Fatalf("producer (mTLS): %v", err)
	}
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"tlsCertFile":   filepath.Join(dir, "client.crt"),
		"tlsKeyFile":    filepath.Join(dir, "client.key"),
		"tlsCACertFile": filepath.Join(dir, "ca.crt"),
	})
	if err != nil {
		t.Fatalf("newKafkaSource (mTLS): %v", err)
	}
	defer src.Close()

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("mTLS streamed %d messages, want %d", total, n)
	}
}

// TestKafkaSource_MutualTLS_WrongClientCert_Live proves a client that does NOT
// present a valid certificate signed by the broker's trusted CA is rejected —
// not silently allowed through and not hung.
func TestKafkaSource_MutualTLS_WrongClientCert_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_MTLS_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_MTLS_BROKER (e.g. localhost:9094) to run this live mutual-TLS negative test")
	}
	dir := certsDir(t)
	if _, err := os.Stat(filepath.Join(dir, "ca.crt")); err != nil {
		t.Skipf("missing ca.crt in %s — run scripts/gen-kafka-test-certs.sh first: %v", dir, err)
	}

	// Generate a throwaway, UNRELATED self-signed cert (not signed by the
	// broker's trusted CA) to present as the client identity.
	badCertPEM, badKeyPEM, err := genSelfSignedClientCert()
	if err != nil {
		t.Fatalf("generate rogue client cert: %v", err)
	}
	tmpDir := t.TempDir()
	badCertFile := filepath.Join(tmpDir, "bad.crt")
	badKeyFile := filepath.Join(tmpDir, "bad.key")
	if err := os.WriteFile(badCertFile, badCertPEM, 0o600); err != nil {
		t.Fatalf("write bad cert: %v", err)
	}
	if err := os.WriteFile(badKeyFile, badKeyPEM, 0o600); err != nil {
		t.Fatalf("write bad key: %v", err)
	}

	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-mtls-bad-%d", ts)
	group := fmt.Sprintf("dp-mtls-bad-grp-%d", ts)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 1000,
		"tlsCertFile":   badCertFile,
		"tlsKeyFile":    badKeyFile,
		"tlsCACertFile": filepath.Join(dir, "ca.crt"),
	})
	// newKafkaSource waits up to 30s for a partition assignment; with a
	// rejected TLS handshake that wait will time out and return an error —
	// exactly the "reject clearly, don't hang forever silently" behavior
	// this test verifies. A nil error here (i.e. it somehow connected) is
	// the failure case.
	if err == nil {
		if src != nil {
			_ = src.Close()
		}
		t.Fatal("newKafkaSource unexpectedly succeeded with a client certificate NOT signed by the broker's trusted CA")
	}
	t.Logf("correctly rejected: %v", err)
}

// TestKafkaSource_SASLPlain_Live proves the source authenticates over
// SASL/PLAIN with valid credentials and streams messages correctly.
//
// Topic setup (create + produce) deliberately goes through the PLAINTEXT
// listener (KAFKA_TEST_BROKER, same cluster, different listener) rather than
// the SASL_PLAINTEXT one: sarama.NewClusterAdmin against this broker's
// SASL_PLAINTEXT listener fails immediately with a bare EOF and no broker-side
// log line at all (a sarama/ClusterAdmin-specific bootstrap quirk, unrelated
// to the source under test — the negative test below proves the broker's
// SASL path itself works correctly via the SAME newKafkaSource/ConsumerGroup
// code path this positive test also uses). What this test actually needs to
// prove — that the SOURCE reads correctly once authenticated via SASL/PLAIN —
// only requires the READ side to go over SASL_PLAINTEXT, so that's all that does.
func TestKafkaSource_SASLPlain_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASL_BROKER")
	plainBroker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" || plainBroker == "" {
		t.Skip("set KAFKA_TEST_SASL_BROKER (e.g. localhost:9095) and KAFKA_TEST_BROKER (e.g. localhost:9092) to run this live SASL/PLAIN test")
	}
	const n = 5
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-sasl-%d", ts)
	group := fmt.Sprintf("dp-sasl-grp-%d", ts)

	setupCfg := sarama.NewConfig()
	setupCfg.Version = sarama.V2_6_0_0
	setupCfg.Producer.Return.Successes = true

	admin, err := sarama.NewClusterAdmin([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("cluster admin (plaintext setup): %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("producer (plaintext setup): %v", err)
	}
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	const passwordEnv = "DP_TEST_KAFKA_SASL_PASSWORD"
	t.Setenv(passwordEnv, "testpass123")

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"saslMechanism": "PLAIN", "saslUser": "testuser", "saslPasswordEnv": passwordEnv,
	})
	if err != nil {
		t.Fatalf("newKafkaSource (SASL/PLAIN): %v", err)
	}
	defer src.Close()

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("SASL/PLAIN streamed %d messages, want %d", total, n)
	}
}

// TestKafkaSource_SASLScram256_Live proves the source authenticates over
// SASL/SCRAM-SHA-256 with valid credentials and streams messages correctly —
// a distinct code path from PLAIN (kafkautil.BuildSaramaConfig wires a
// SCRAMClientGeneratorFunc via xdg-go/scram for SCRAM mechanisms). The SCRAM
// user is provisioned dynamically in cluster metadata (not a static JAAS
// entry like PLAIN) by `make test-integration-up` via kafka-configs.sh;
// KAFKA_TEST_SCRAM_USER/KAFKA_TEST_SCRAM_PASSWORD must match what was
// provisioned (Makefile defaults: testuser256/testpass256).
func TestKafkaSource_SASLScram256_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASL_BROKER")
	plainBroker := os.Getenv("KAFKA_TEST_BROKER")
	user := os.Getenv("KAFKA_TEST_SCRAM_USER")
	pass := os.Getenv("KAFKA_TEST_SCRAM_PASSWORD")
	if broker == "" || plainBroker == "" || user == "" || pass == "" {
		t.Skip("set KAFKA_TEST_SASL_BROKER, KAFKA_TEST_BROKER, KAFKA_TEST_SCRAM_USER, KAFKA_TEST_SCRAM_PASSWORD to run this live SCRAM-SHA-256 test (provisioned by `make test-integration-up`)")
	}
	const n = 5
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-scram-%d", ts)
	group := fmt.Sprintf("dp-scram-grp-%d", ts)

	setupCfg := sarama.NewConfig()
	setupCfg.Version = sarama.V2_6_0_0
	setupCfg.Producer.Return.Successes = true
	admin, err := sarama.NewClusterAdmin([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("cluster admin (plaintext setup): %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("producer (plaintext setup): %v", err)
	}
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	const passwordEnv = "DP_TEST_KAFKA_SCRAM_PASSWORD"
	t.Setenv(passwordEnv, pass)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"saslMechanism": "SCRAM-SHA-256", "saslUser": user, "saslPasswordEnv": passwordEnv,
	})
	if err != nil {
		t.Fatalf("newKafkaSource (SASL/SCRAM-SHA-256): %v", err)
	}
	defer src.Close()

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("SASL/SCRAM-SHA-256 streamed %d messages, want %d", total, n)
	}
}

// TestKafkaSource_SASLPlain_WrongPassword_Live proves a wrong password is
// rejected with a clear error, not silently allowed or hung.
func TestKafkaSource_SASLPlain_WrongPassword_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASL_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_SASL_BROKER (e.g. localhost:9095) to run this live SASL/PLAIN negative test")
	}
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-sasl-bad-%d", ts)
	group := fmt.Sprintf("dp-sasl-bad-grp-%d", ts)

	const passwordEnv = "DP_TEST_KAFKA_SASL_WRONG_PASSWORD"
	t.Setenv(passwordEnv, "totally-wrong-password")

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 1000,
		"saslMechanism": "PLAIN", "saslUser": "testuser", "saslPasswordEnv": passwordEnv,
	})
	if err == nil {
		if src != nil {
			_ = src.Close()
		}
		t.Fatal("newKafkaSource unexpectedly succeeded with a WRONG SASL password")
	}
	t.Logf("correctly rejected: %v", err)
}

// TestKafkaSource_SASLScram512_Live mirrors TestKafkaSource_SASLScram256_Live
// for SCRAM-SHA-512 — a distinct hash function wired through the exact same
// SCRAMClientGeneratorFunc code path in kafkautil.BuildSaramaConfig, so this
// closes the "only SHA-256 was tested" gap rather than assuming SHA-512
// behaves identically by inspection alone.
func TestKafkaSource_SASLScram512_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASL_BROKER")
	plainBroker := os.Getenv("KAFKA_TEST_BROKER")
	user := os.Getenv("KAFKA_TEST_SCRAM512_USER")
	pass := os.Getenv("KAFKA_TEST_SCRAM512_PASSWORD")
	if broker == "" || plainBroker == "" || user == "" || pass == "" {
		t.Skip("set KAFKA_TEST_SASL_BROKER, KAFKA_TEST_BROKER, KAFKA_TEST_SCRAM512_USER, KAFKA_TEST_SCRAM512_PASSWORD to run this live SCRAM-SHA-512 test (provisioned by `make test-integration-up`)")
	}
	const n = 5
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-scram512-%d", ts)
	group := fmt.Sprintf("dp-scram512-grp-%d", ts)

	setupCfg := sarama.NewConfig()
	setupCfg.Version = sarama.V2_6_0_0
	setupCfg.Producer.Return.Successes = true
	admin, err := sarama.NewClusterAdmin([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("cluster admin (plaintext setup): %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("producer (plaintext setup): %v", err)
	}
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	const passwordEnv = "DP_TEST_KAFKA_SCRAM512_PASSWORD"
	t.Setenv(passwordEnv, pass)

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"saslMechanism": "SCRAM-SHA-512", "saslUser": user, "saslPasswordEnv": passwordEnv,
	})
	if err != nil {
		t.Fatalf("newKafkaSource (SASL/SCRAM-SHA-512): %v", err)
	}
	defer src.Close()

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("SASL/SCRAM-SHA-512 streamed %d messages, want %d", total, n)
	}
}

// TestKafkaSource_SASLSSL_Live proves the SASL_SSL combination (SASL/PLAIN
// authentication layered on top of an ENCRYPTED TLS transport, the standard
// production deployment shape — distinct from plain SASL_PLAINTEXT and from
// mutual-TLS-without-SASL, both already covered above) works end-to-end: the
// source authenticates via saslMechanism/saslUser/saslPasswordEnv AND
// verifies the broker's certificate via tlsCACertFile (no client cert is
// presented — ssl.client.auth=none on this listener, since SASL already
// establishes client identity).
func TestKafkaSource_SASLSSL_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASLSSL_BROKER")
	plainBroker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" || plainBroker == "" {
		t.Skip("set KAFKA_TEST_SASLSSL_BROKER (e.g. localhost:9096) and KAFKA_TEST_BROKER to run this live SASL_SSL test")
	}
	dir := certsDir(t)
	if _, err := os.Stat(filepath.Join(dir, "ca.crt")); err != nil {
		t.Skipf("missing ca.crt in %s — run scripts/gen-kafka-test-certs.sh first: %v", dir, err)
	}

	const n = 5
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-saslssl-%d", ts)
	group := fmt.Sprintf("dp-saslssl-grp-%d", ts)

	setupCfg := sarama.NewConfig()
	setupCfg.Version = sarama.V2_6_0_0
	setupCfg.Producer.Return.Successes = true
	admin, err := sarama.NewClusterAdmin([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("cluster admin (plaintext setup): %v", err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(topic) })

	prod, err := sarama.NewSyncProducer([]string{plainBroker}, setupCfg)
	if err != nil {
		t.Fatalf("producer (plaintext setup): %v", err)
	}
	for i := 1; i <= n; i++ {
		val, _ := json.Marshal(map[string]interface{}{"id": i})
		if _, _, perr := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(val)}); perr != nil {
			t.Fatalf("produce %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	const passwordEnv = "DP_TEST_KAFKA_SASLSSL_PASSWORD"
	t.Setenv(passwordEnv, "testpass123")

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 2000,
		"saslMechanism": "PLAIN", "saslUser": "testuser", "saslPasswordEnv": passwordEnv,
		"tlsCACertFile": filepath.Join(dir, "ca.crt"), // no tlsCertFile/tlsKeyFile: ssl.client.auth=none on this listener
	})
	if err != nil {
		t.Fatalf("newKafkaSource (SASL_SSL): %v", err)
	}
	defer src.Close()

	total := 0
	deadline := time.Now().Add(20 * time.Second)
	for total < n && time.Now().Before(deadline) {
		b, rerr := src.ReadBatch(10)
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
		if b != nil {
			total += b.Len()
		}
	}
	if total != n {
		t.Fatalf("SASL_SSL streamed %d messages, want %d", total, n)
	}
}

// TestKafkaSource_SASLSSL_WrongPassword_Live proves that, even over an
// encrypted transport, a wrong SASL password is still rejected with a clear
// error rather than the TLS layer somehow masking the authentication failure.
func TestKafkaSource_SASLSSL_WrongPassword_Live(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_SASLSSL_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_SASLSSL_BROKER (e.g. localhost:9096) to run this live SASL_SSL negative test")
	}
	dir := certsDir(t)
	if _, err := os.Stat(filepath.Join(dir, "ca.crt")); err != nil {
		t.Skipf("missing ca.crt in %s — run scripts/gen-kafka-test-certs.sh first: %v", dir, err)
	}
	ts := time.Now().UnixNano()
	topic := fmt.Sprintf("dp-saslssl-bad-%d", ts)
	group := fmt.Sprintf("dp-saslssl-bad-grp-%d", ts)

	const passwordEnv = "DP_TEST_KAFKA_SASLSSL_WRONG_PASSWORD"
	t.Setenv(passwordEnv, "totally-wrong-password")

	src, err := newKafkaSource(map[string]interface{}{
		"brokers": broker, "topic": topic, "groupId": group,
		"startOffset": "earliest", "deserializer": "json", "pollTimeoutMs": 1000,
		"saslMechanism": "PLAIN", "saslUser": "testuser", "saslPasswordEnv": passwordEnv,
		"tlsCACertFile": filepath.Join(dir, "ca.crt"),
	})
	if err == nil {
		if src != nil {
			_ = src.Close()
		}
		t.Fatal("newKafkaSource unexpectedly succeeded over SASL_SSL with a WRONG password")
	}
	t.Logf("correctly rejected: %v", err)
}
