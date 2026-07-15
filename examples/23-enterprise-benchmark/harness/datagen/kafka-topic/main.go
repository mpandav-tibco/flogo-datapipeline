// Command kafka-topic (re)creates an empty Kafka topic: it deletes the topic if
// present, then creates it fresh with the requested partition count. Used to
// prepare the 'orders' source topic before seeding and to reset sink topics
// between benchmark cases.
//
// Usage: kafka-topic <brokers> <topic> [partitions]
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: kafka-topic <brokers> <topic> [partitions]")
		os.Exit(1)
	}
	brokers := []string{os.Args[1]}
	topic := os.Args[2]
	parts := int32(1)
	if len(os.Args) > 3 {
		if n, err := strconv.Atoi(os.Args[3]); err == nil {
			parts = int32(n)
		}
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin: %v\n", err)
		os.Exit(1)
	}
	defer admin.Close()

	_ = admin.DeleteTopic(topic) // ignore "unknown topic" on first run
	time.Sleep(750 * time.Millisecond)
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions: parts, ReplicationFactor: 1,
	}, false); err != nil {
		// A delete/create race can report "already exists" — not fatal.
		fmt.Fprintf(os.Stderr, "create %s (non-fatal): %v\n", topic, err)
	}
	fmt.Printf("reset topic %s (%d partitions)\n", topic, parts)
}
