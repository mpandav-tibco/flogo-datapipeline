// Package kafkautil provides shared helpers for the kafka source and sink packages.
// It builds sarama.Config objects from pipeline config maps and implements
// SCRAM-SHA-256/512 authentication using the xdg-go/scram library.
package kafkautil

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// SHA256 and SHA512 are hash-generator functions for SCRAM authentication.
var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient implements sarama.SCRAMClient via the xdg-go/scram library.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn scram.HashGeneratorFcn
}

var _ sarama.SCRAMClient = (*XDGSCRAMClient)(nil)

// Begin initialises the SCRAM exchange with the given credentials.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step processes a SCRAM server challenge and returns the client response.
func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

// Done reports whether the SCRAM exchange is complete.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// BuildSaramaConfig constructs a *sarama.Config with TLS and SASL settings
// extracted from a pipeline config map. Both the source and sink packages call
// this function to avoid duplicating TLS/SASL parsing logic.
//
// Config keys consumed:
//
//	saslMechanism   string  "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
//	saslUser        string  SASL username
//	saslPasswordEnv string  Env var name that holds the SASL password
//	tlsCertFile     string  Path to TLS client certificate PEM file
//	tlsKeyFile      string  Path to TLS client key PEM file
//	tlsCACertFile   string  Path to TLS CA certificate PEM file
func BuildSaramaConfig(cfg map[string]interface{}) (*sarama.Config, error) {
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_6_0_0

	// SASL configuration.
	if mech, ok := cfg["saslMechanism"].(string); ok && mech != "" {
		user, _ := cfg["saslUser"].(string)
		passwordEnv, _ := cfg["saslPasswordEnv"].(string)
		password := os.Getenv(passwordEnv)
		if user == "" {
			return nil, fmt.Errorf("kafkautil: saslUser is required when saslMechanism is set")
		}
		if password == "" {
			return nil, fmt.Errorf("kafkautil: env var %q (saslPasswordEnv) is empty or unset", passwordEnv)
		}
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = user
		sc.Net.SASL.Password = password
		switch mech {
		case "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		default:
			return nil, fmt.Errorf("kafkautil: unknown saslMechanism %q (must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", mech)
		}
	}

	// TLS configuration.
	certFile, _ := cfg["tlsCertFile"].(string)
	keyFile, _ := cfg["tlsKeyFile"].(string)
	caFile, _ := cfg["tlsCACertFile"].(string)

	if certFile != "" || caFile != "" {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("kafkautil: TLS load key pair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		} else if certFile != "" || keyFile != "" {
			return nil, fmt.Errorf("kafkautil: both tlsCertFile and tlsKeyFile must be set together")
		}

		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("kafkautil: read CA cert %q: %w", caFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("kafkautil: failed to parse CA certificate from %q", caFile)
			}
			tlsCfg.RootCAs = pool
		}

		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tlsCfg
	}

	return sc, nil
}
