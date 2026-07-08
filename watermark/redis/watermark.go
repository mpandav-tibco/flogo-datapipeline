// Package redis registers a Redis-backed watermark store.
// Each pipeline's mark is stored as a single Redis string key containing the
// JSON-serialised watermark.Mark. No TTL is set by default — watermarks are
// permanent state that must survive indefinitely until a pipeline is reset.
//
// Config keys:
//
//	addr        string  Redis host:port (default "localhost:6379")
//	passwordEnv string  Name of an env var holding the Redis password (optional)
//	keyPrefix   string  Key namespace (default "flogo:pipeline:hwm")
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/redis"
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

func init() {
	watermark.Register("redis", newRedisStore)
}

type redisStore struct {
	client    *goredis.Client
	keyPrefix string
}

func newRedisStore(cfg map[string]interface{}) (watermark.Store, error) {
	addr, _ := cfg["addr"].(string)
	if addr == "" {
		addr = "localhost:6379"
	}
	keyPrefix, _ := cfg["keyPrefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "flogo:pipeline:hwm"
	}

	passwordEnv, _ := cfg["passwordEnv"].(string)
	password := ""
	if passwordEnv != "" {
		password = os.Getenv(passwordEnv)
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis watermark: ping failed: %w", err)
	}

	return &redisStore{
		client:    client,
		keyPrefix: keyPrefix,
	}, nil
}

func (r *redisStore) key(pipelineID string) string {
	return r.keyPrefix + ":" + pipelineID
}

func (r *redisStore) Load(pipelineID string) (*watermark.Mark, error) {
	ctx := context.Background()
	data, err := r.client.Get(ctx, r.key(pipelineID)).Bytes()
	if err == goredis.Nil {
		return nil, nil // no mark — first run, full-table load
	}
	if err != nil {
		return nil, fmt.Errorf("redis watermark: get %q: %w", pipelineID, err)
	}
	var m watermark.Mark
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("redis watermark: parse %q: %w", pipelineID, err)
	}
	return &m, nil
}

func (r *redisStore) Save(pipelineID string, m *watermark.Mark) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("redis watermark: marshal %q: %w", pipelineID, err)
	}
	// TTL of 0 means no expiry — watermarks are permanent until explicitly reset.
	ctx := context.Background()
	if err := r.client.Set(ctx, r.key(pipelineID), data, 0).Err(); err != nil {
		return fmt.Errorf("redis watermark: set %q: %w", pipelineID, err)
	}
	return nil
}

func (r *redisStore) Close() error {
	return r.client.Close()
}
