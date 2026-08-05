package acmeplugins

import (
	"fmt"
	"strings"

	ext "github.com/mpandav-tibco/flogo-datapipeline/extension"
)

func init() { ext.RegisterTransform("uppercase", newUppercase) }

// uppercase upper-cases the string value of one configured column, in place.
type uppercase struct{ column string }

func newUppercase(cfg map[string]interface{}) (ext.Transform, error) {
	col, _ := cfg["column"].(string)
	if col == "" {
		return nil, fmt.Errorf("uppercase: 'column' is required")
	}
	return &uppercase{column: col}, nil
}

func (u *uppercase) Name() string { return "uppercase" }

func (u *uppercase) Apply(in *ext.RecordBatch) (*ext.RecordBatch, error) {
	idx := in.ColIndex(u.column)
	if idx < 0 {
		return nil, fmt.Errorf("uppercase: column %q not present in batch", u.column)
	}
	for _, row := range in.Rows {
		if s, ok := row[idx].(string); ok {
			row[idx] = strings.ToUpper(s)
		}
	}
	return in, nil
}

func (u *uppercase) Close() error { return nil }
