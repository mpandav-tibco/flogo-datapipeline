package pipeline

import "testing"

func TestValidateRejectsKeyedWindowUnderSharding(t *testing.T) {
	base := func() *Definition {
		return &Definition{Source: SourceDef{Type: "csv"}, Sink: SinkDef{Type: "jsonlines"}}
	}
	keyed := TransformDef{Type: "window", Extra: map[string]interface{}{"groupBy": []interface{}{"sensor"}}}
	global := TransformDef{Type: "window", Extra: map[string]interface{}{}}

	cases := []struct {
		name    string
		mode    string
		tf      TransformDef
		wantErr bool
	}{
		{"sharded+keyed window rejected", "sharded", keyed, true},
		{"distributed+keyed window rejected", "distributed", keyed, true},
		{"single+keyed window allowed", "single", keyed, false},
		{"sharded+global window (no key) allowed", "sharded", global, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base()
			d.Scaling.Mode = c.mode
			d.Transforms = []TransformDef{c.tf}
			err := d.Validate()
			if c.wantErr && err == nil {
				t.Fatalf("expected a validation error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
		})
	}
}

func TestValidateRejectsCheckpointCommitWithoutBackend(t *testing.T) {
	base := func() *Definition {
		return &Definition{Source: SourceDef{Type: "kafka"}, Sink: SinkDef{Type: "jsonlines"}}
	}
	// commitMode checkpoint + no checkpoint backend → error (silent-loss footgun).
	d := base()
	d.Source.Options = map[string]interface{}{"commitMode": "checkpoint"}
	if err := d.Validate(); err == nil {
		t.Error("expected error for commitMode=checkpoint without a checkpoint backend")
	}
	// commitMode checkpoint + durable file checkpoint → ok.
	d = base()
	d.Source.Options = map[string]interface{}{"commitMode": "checkpoint"}
	d.Checkpoint = CheckpointDef{Type: "file"}
	if err := d.Validate(); err != nil {
		t.Errorf("commitMode=checkpoint with file checkpoint should be valid: %v", err)
	}
	// default auto commit needs no checkpoint.
	d = base()
	d.Source.Options = map[string]interface{}{"commitMode": "auto"}
	if err := d.Validate(); err != nil {
		t.Errorf("commitMode=auto should be valid without a checkpoint: %v", err)
	}
}

func TestValidateRequiresCheckpointCommitForWindowedKafka(t *testing.T) {
	base := func() *Definition {
		return &Definition{
			Source:     SourceDef{Type: "kafka"},
			Sink:       SinkDef{Type: "jsonlines"},
			Transforms: []TransformDef{{Type: "window"}},
			Checkpoint: CheckpointDef{Type: "file"},
		}
	}
	// windowed Kafka + checkpoint backend + auto commit → rejected (uncoordinated resume).
	d := base()
	d.Source.Options = map[string]interface{}{"commitMode": "auto"}
	if err := d.Validate(); err == nil {
		t.Error("expected error for windowed Kafka + checkpoint backend in auto commit mode")
	}
	// unset commitMode defaults to auto → also rejected.
	d = base()
	if err := d.Validate(); err == nil {
		t.Error("expected error for windowed Kafka + checkpoint backend with unset (auto) commitMode")
	}
	// commitMode checkpoint → allowed (offset co-committed with window snapshot).
	d = base()
	d.Source.Options = map[string]interface{}{"commitMode": "checkpoint"}
	if err := d.Validate(); err != nil {
		t.Errorf("windowed Kafka with checkpoint commit + backend should be valid: %v", err)
	}
	// windowed Kafka WITHOUT a checkpoint backend → allowed (windows start fresh; nothing to misalign).
	d = base()
	d.Checkpoint = CheckpointDef{}
	if err := d.Validate(); err != nil {
		t.Errorf("windowed Kafka without a checkpoint backend should be valid: %v", err)
	}
	// windowed NON-Kafka (file) source in auto mode + backend → allowed (deterministic Skip replay).
	d = base()
	d.Source = SourceDef{Type: "csv"}
	if err := d.Validate(); err != nil {
		t.Errorf("windowed file source with checkpoint should be valid regardless of commitMode: %v", err)
	}
}
