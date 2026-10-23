package kafka

import (
	"io"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func fakeSource(fail bool, msgs ...string) *kafkaSource {
	h := &consumerGroupHandler{msgCh: make(chan msgEnvelope, len(msgs))}
	for i, m := range msgs {
		h.msgCh <- msgEnvelope{msg: &sarama.ConsumerMessage{Partition: 0, Offset: int64(i), Value: []byte(m)}}
	}
	return &kafkaSource{
		cfg:               &kafkaSourceConfig{topic: "t", pollMs: 50, mode: "streaming"},
		handler:           h,
		commitMode:        "checkpoint",
		pending:           map[int32]int64{},
		failOnDeserialize: fail,
	}
}

func TestDeserializeErrorSkipAdvancesOffset(t *testing.T) {
	ks := fakeSource(false, `{"a":1}`, `not-json`)
	b, err := ks.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if b.Len() != 1 || ks.DeserializeErrors() != 1 {
		t.Fatalf("rows=%d skipped=%d; want 1/1", b.Len(), ks.DeserializeErrors())
	}
	// The undecodable message is the last one on its partition; the commit
	// target must still move past it so it is not redelivered on restart.
	if got := ks.pending[0]; got != 2 {
		t.Fatalf("pending offset=%d; want 2 (past the skipped message)", got)
	}
}

func TestDeserializeErrorFail(t *testing.T) {
	ks := fakeSource(true, `{"a":1}`, `not-json`)
	_, err := ks.ReadBatch(10)
	if err == nil || !strings.Contains(err.Error(), "offset=1") {
		t.Fatalf("want deserialize error at offset 1, got %v", err)
	}
}

func TestOnDeserializeErrorValidation(t *testing.T) {
	_, err := newKafkaSource(map[string]interface{}{
		"brokers": []interface{}{"127.0.0.1:1"}, "topic": "t", "groupId": "g", "onDeserializeError": "explode",
	})
	if err == nil || !strings.Contains(err.Error(), "onDeserializeError") {
		t.Fatalf("want validation error, got %v", err)
	}
}
