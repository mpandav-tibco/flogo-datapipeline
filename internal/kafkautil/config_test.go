package kafkautil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// writePEM writes PEM-encoded bytes to a temp file and returns its path.
func writePEM(t *testing.T, pemBytes []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "kafkautil_test_*.pem")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.Write(pemBytes); err != nil {
		t.Fatalf("write PEM: %v", err)
	}
	f.Close()
	return f.Name()
}

// selfSignedCert generates an in-memory self-signed cert+key pair (ECDSA P-256)
// and returns their PEM-encoded bytes.
func selfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// ─── No SASL / clean config ───────────────────────────────────────────────────

func TestBuildSaramaConfig_NoSASL(t *testing.T) {
	cfg, err := BuildSaramaConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("BuildSaramaConfig (no SASL): %v", err)
	}
	if cfg == nil {
		t.Fatal("returned nil config")
	}
	if cfg.Net.SASL.Enable {
		t.Error("SASL should be disabled when no saslMechanism is set")
	}
	if cfg.Net.TLS.Enable {
		t.Error("TLS should be disabled when no TLS config is provided")
	}
	if cfg.Version != sarama.V2_6_0_0 {
		t.Errorf("expected version V2_6_0_0, got %v", cfg.Version)
	}
}

// ─── SCRAM-SHA-256 ────────────────────────────────────────────────────────────

func TestBuildSaramaConfig_SCRAM_SHA256(t *testing.T) {
	const envVar = "KAFKA_SASL_PASS_SHA256_TEST"
	t.Setenv(envVar, "s3cr3t")

	cfg, err := BuildSaramaConfig(map[string]interface{}{
		"saslMechanism":   "SCRAM-SHA-256",
		"saslUser":        "testuser",
		"saslPasswordEnv": envVar,
	})
	if err != nil {
		t.Fatalf("BuildSaramaConfig SCRAM-SHA-256: %v", err)
	}
	if !cfg.Net.SASL.Enable {
		t.Error("SASL should be enabled")
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA256 {
		t.Errorf("mechanism = %v, want SCRAM-SHA-256", cfg.Net.SASL.Mechanism)
	}
	if cfg.Net.SASL.User != "testuser" {
		t.Errorf("user = %q, want testuser", cfg.Net.SASL.User)
	}
	if cfg.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Error("SCRAMClientGeneratorFunc should be set for SCRAM-SHA-256")
	}
}

// ─── SCRAM-SHA-512 ────────────────────────────────────────────────────────────

func TestBuildSaramaConfig_SCRAM_SHA512(t *testing.T) {
	const envVar = "KAFKA_SASL_PASS_SHA512_TEST"
	t.Setenv(envVar, "anothersecret")

	cfg, err := BuildSaramaConfig(map[string]interface{}{
		"saslMechanism":   "SCRAM-SHA-512",
		"saslUser":        "user512",
		"saslPasswordEnv": envVar,
	})
	if err != nil {
		t.Fatalf("BuildSaramaConfig SCRAM-SHA-512: %v", err)
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA512 {
		t.Errorf("mechanism = %v, want SCRAM-SHA-512", cfg.Net.SASL.Mechanism)
	}
	if cfg.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Error("SCRAMClientGeneratorFunc should be set for SCRAM-SHA-512")
	}
}

// ─── PLAIN ────────────────────────────────────────────────────────────────────

func TestBuildSaramaConfig_PLAIN(t *testing.T) {
	const envVar = "KAFKA_SASL_PASS_PLAIN_TEST"
	t.Setenv(envVar, "plainpass")

	cfg, err := BuildSaramaConfig(map[string]interface{}{
		"saslMechanism":   "PLAIN",
		"saslUser":        "plainuser",
		"saslPasswordEnv": envVar,
	})
	if err != nil {
		t.Fatalf("BuildSaramaConfig PLAIN: %v", err)
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypePlaintext {
		t.Errorf("mechanism = %v, want PLAIN", cfg.Net.SASL.Mechanism)
	}
}

// ─── TLS: CA cert only ────────────────────────────────────────────────────────

func TestBuildSaramaConfig_TLS_CAOnly(t *testing.T) {
	certPEM, _ := selfSignedCert(t)
	caFile := writePEM(t, certPEM)
	defer os.Remove(caFile)

	cfg, err := BuildSaramaConfig(map[string]interface{}{
		"tlsCACertFile": caFile,
	})
	if err != nil {
		t.Fatalf("BuildSaramaConfig TLS CA-only: %v", err)
	}
	if !cfg.Net.TLS.Enable {
		t.Error("TLS should be enabled when tlsCACertFile is set")
	}
	if cfg.Net.TLS.Config == nil {
		t.Error("TLS.Config should not be nil")
	}
	if cfg.Net.TLS.Config.RootCAs == nil {
		t.Error("RootCAs should be populated from CA cert file")
	}
}

// ─── TLS: cert + key pair ─────────────────────────────────────────────────────

func TestBuildSaramaConfig_TLS_CertKeyPair(t *testing.T) {
	certPEM, keyPEM := selfSignedCert(t)
	certFile := writePEM(t, certPEM)
	keyFile := writePEM(t, keyPEM)
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	cfg, err := BuildSaramaConfig(map[string]interface{}{
		"tlsCertFile": certFile,
		"tlsKeyFile":  keyFile,
	})
	if err != nil {
		t.Fatalf("BuildSaramaConfig TLS cert+key: %v", err)
	}
	if !cfg.Net.TLS.Enable {
		t.Error("TLS should be enabled")
	}
	if len(cfg.Net.TLS.Config.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Net.TLS.Config.Certificates))
	}
}

// ─── Error: missing key file when cert file is set ────────────────────────────

func TestBuildSaramaConfig_MissingKeyFile_Error(t *testing.T) {
	certPEM, _ := selfSignedCert(t)
	certFile := writePEM(t, certPEM)
	defer os.Remove(certFile)

	// Provide cert but NOT key — should return an error.
	_, err := BuildSaramaConfig(map[string]interface{}{
		"tlsCertFile": certFile,
		// tlsKeyFile intentionally omitted
	})
	if err == nil {
		t.Fatal("expected error when tlsCertFile is set without tlsKeyFile, got nil")
	}
}

// ─── Error: unknown SASL mechanism ───────────────────────────────────────────

func TestBuildSaramaConfig_UnknownMechanism_Error(t *testing.T) {
	const envVar = "KAFKA_SASL_PASS_UNKNOWN_TEST"
	t.Setenv(envVar, "pass")

	_, err := BuildSaramaConfig(map[string]interface{}{
		"saslMechanism":   "KERBEROS",
		"saslUser":        "u",
		"saslPasswordEnv": envVar,
	})
	if err == nil {
		t.Fatal("expected error for unknown SASL mechanism, got nil")
	}
}

// ─── Error: empty password env var ───────────────────────────────────────────

func TestBuildSaramaConfig_EmptyPasswordEnv_Error(t *testing.T) {
	// Do not set the env var — it will be empty/unset.
	const envVar = "KAFKA_SASL_PASS_EMPTY_GUARANTEED_NOT_SET_XYZ"
	os.Unsetenv(envVar)

	_, err := BuildSaramaConfig(map[string]interface{}{
		"saslMechanism":   "PLAIN",
		"saslUser":        "u",
		"saslPasswordEnv": envVar,
	})
	if err == nil {
		t.Fatal("expected error for empty password env var, got nil")
	}
}

// ─── SCRAMClientGeneratorFunc produces a working client ──────────────────────

func TestXDGSCRAMClient_InterfaceSatisfied(t *testing.T) {
	// Verify XDGSCRAMClient satisfies sarama.SCRAMClient at compile time.
	// The var-check in config.go already does this, but repeat here for clarity.
	var _ sarama.SCRAMClient = (*XDGSCRAMClient)(nil)
}
