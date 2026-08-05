package scaling

import (
	"context"
	"testing"

	_ "github.com/lib/pq" // postgres driver for BuildPostgresQueue

	"github.com/mpandav-tibco/flogo-datapipeline/partition/workqueue"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func TestResolveFromEnv(t *testing.T) {
	t.Setenv("INSTANCE_INDEX", "2")
	t.Setenv("INSTANCE_COUNT", "5")
	c := &Config{}
	c.ResolveFromEnv()
	if c.InstanceIndex != 2 || c.InstanceCount != 5 {
		t.Fatalf("resolved = %d/%d, want 2/5", c.InstanceIndex, c.InstanceCount)
	}

	// Invalid values are ignored, leaving the existing config untouched.
	t.Setenv("INSTANCE_INDEX", "not-a-number")
	t.Setenv("INSTANCE_COUNT", "nope")
	c2 := &Config{InstanceIndex: 7, InstanceCount: 9}
	c2.ResolveFromEnv()
	if c2.InstanceIndex != 7 || c2.InstanceCount != 9 {
		t.Errorf("invalid env mutated config: %d/%d", c2.InstanceIndex, c2.InstanceCount)
	}
}

func TestBuildQueue_MemoryDefault(t *testing.T) {
	q, err := BuildQueue(Config{}, "pipe-1")
	if err != nil {
		t.Fatalf("BuildQueue memory: %v", err)
	}
	defer q.Close()
	if q == nil {
		t.Fatal("BuildQueue returned nil")
	}
}

func TestBuildQueue_PostgresErrors(t *testing.T) {
	if _, err := BuildQueue(Config{CoordinatorType: "postgres"}, "p"); err == nil {
		t.Fatal("postgres coordinator with empty DSN expected an error")
	}
	if _, err := BuildPostgresQueue(Config{CoordinatorType: "postgres"}, "p"); err == nil {
		t.Fatal("BuildPostgresQueue with empty DSN expected an error")
	}
}

func TestCoordinatorEnqueueDirect(t *testing.T) {
	q := workqueue.NewMemoryQueue()
	c := NewCoordinator(Config{MaxRetries: 2}, q)
	parts := []source.Partition{{ID: "a"}, {ID: "b"}}
	if err := c.Enqueue(context.Background(), parts); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	stats, err := q.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[workqueue.StatusPending] != 2 {
		t.Errorf("pending = %d, want 2", stats[workqueue.StatusPending])
	}
}
