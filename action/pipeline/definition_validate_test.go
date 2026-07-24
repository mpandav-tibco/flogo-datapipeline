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
