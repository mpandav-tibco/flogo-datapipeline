// Package redis registers a Redis-backed checkpoint store for multi-instance
// pipelines. Partition checkpoint tokens are stored as Redis hashes under
// keyPrefix:pipelineId:partitionId, with an optional TTL for automatic cleanup.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/redis"
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
)

func init() {
	checkpoint.Register("redis", newRedisCheckpoint)
}

type redisCheckpoint struct {
	client    *goredis.Client
	keyPrefix string
	ttl       time.Duration
}

func newRedisCheckpoint(cfg map[string]interface{}) (checkpoint.Checkpoint, error) {
	addr, _ := cfg["addr"].(string)
	if addr == "" {
		addr = "localhost:6379"
	}
	keyPrefix, _ := cfg["keyPrefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "flogo:pipeline:ckpt"
	}

	passwordEnv, _ := cfg["passwordEnv"].(string)
	password := ""
	if passwordEnv != "" {
		password = os.Getenv(passwordEnv)
	}

	ttlSeconds := 86400 * 7 // 7 days default
	if ttl, ok := cfg["ttlSeconds"].(int); ok && ttl > 0 {
		ttlSeconds = ttl
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis checkpoint: ping failed: %w", err)
	}

	return &redisCheckpoint{
		client:    client,
		keyPrefix: keyPrefix,
		ttl:       time.Duration(ttlSeconds) * time.Second,
	}, nil
}

func (r *redisCheckpoint) key(partitionID string) string {
	return r.keyPrefix + ":" + partitionID
}

func (r *redisCheckpoint) Save(partitionID string, t *checkpoint.Token) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("redis checkpoint: marshal: %w", err)
	}
	ctx := context.Background()
	return r.client.Set(ctx, r.key(partitionID), data, r.ttl).Err()
}

func (r *redisCheckpoint) Load(partitionID string) (*checkpoint.Token, error) {
	ctx := context.Background()
	data, err := r.client.Get(ctx, r.key(partitionID)).Bytes()
	if err == goredis.Nil {
		return nil, nil // no checkpoint
	}
	if err != nil {
		return nil, fmt.Errorf("redis checkpoint: get: %w", err)
	}
	var t checkpoint.Token
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("redis checkpoint: parse: %w", err)
	}
	return &t, nil
}

func (r *redisCheckpoint) Delete(partitionID string) error {
	return r.client.Del(context.Background(), r.key(partitionID)).Err()
}

func (r *redisCheckpoint) Close() error {
	return r.client.Close()
}
