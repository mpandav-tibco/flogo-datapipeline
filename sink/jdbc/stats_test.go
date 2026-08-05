package jdbc

import "testing"

// TestSinkStats_NotDeleteCapable proves a plain (no opColumn) sink reports
// RowsDeleted=-1 ("not tracked"), so lineage/audit correctly omit the CDC
// stats facet for pipelines with no delete concept.
func TestSinkStats_NotDeleteCapable(t *testing.T) {
	s := &jdbcSink{}
	st := s.SinkStats()
	if st.RowsDeleted != -1 {
		t.Errorf("RowsDeleted = %d, want -1 (not tracked, no opColumn)", st.RowsDeleted)
	}
}

// TestSinkStats_DeleteCapable proves a delete-capable sink (opColumn set)
// reports its accumulated rowsDeleted counter via SinkStats.
func TestSinkStats_DeleteCapable(t *testing.T) {
	s := &jdbcSink{opColumn: "__op", rowsDeleted: 7}
	st := s.SinkStats()
	if st.RowsDeleted != 7 {
		t.Errorf("RowsDeleted = %d, want 7", st.RowsDeleted)
	}
}
