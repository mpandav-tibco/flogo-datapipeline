package build

import "testing"

func has(imports []string, want string) bool {
	for _, i := range imports {
		if i == want {
			return true
		}
	}
	return false
}

// csv -> s3 (parquet format) must link the CSV source, the object-store sink,
// the S3 provider and the parquet encoder — and nothing relational, no CGO.
func TestDeriveImports_CSVtoS3(t *testing.T) {
	data := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "s3-sink", "format": "parquet"},
	}
	imp, cgo := DeriveImports(data)
	if cgo {
		t.Fatal("csv->s3 should not require CGO")
	}
	for _, w := range []string{
		module + "action/pipeline", module + "source/csv", module + "sink/objstore",
		module + "objstore/s3", module + "sink/parquet",
		"github.com/project-flogo/contrib/trigger/timer",
	} {
		if !has(imp, w) {
			t.Errorf("missing expected import %s in %v", w, imp)
		}
	}
	if has(imp, "github.com/lib/pq") {
		t.Error("csv->s3 must not link the postgres driver")
	}
	if has(imp, module+"source/kafka") {
		t.Error("csv->s3 must not link the kafka source")
	}
}

// JDBC sinks pull in their driver; all bundled drivers are pure Go (never CGO).
func TestDeriveImports_JDBCDriversAndCGO(t *testing.T) {
	pg := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "jdbc", "driver": "postgres"},
	}
	imp, cgo := DeriveImports(pg)
	if cgo {
		t.Error("postgres must not require CGO")
	}
	if !has(imp, "github.com/lib/pq") {
		t.Errorf("missing lib/pq in %v", imp)
	}

	mssql := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "jdbc", "driver": "sqlserver"},
	}
	impM, cgoM := DeriveImports(mssql)
	if cgoM {
		t.Error("sqlserver must not require CGO (go-mssqldb is pure Go)")
	}
	if !has(impM, "github.com/microsoft/go-mssqldb") {
		t.Errorf("missing go-mssqldb driver in %v", impM)
	}

	ora := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "jdbc", "driver": "oracle"},
	}
	impO, cgoO := DeriveImports(ora)
	if cgoO {
		t.Error("oracle must not require CGO (go-ora is pure Go)")
	}
	if !has(impO, "github.com/sijms/go-ora/v2") {
		t.Errorf("missing go-ora driver in %v", impO)
	}
}

// Every declared transform must be linked; unused ones must not.
func TestDeriveImports_Transforms(t *testing.T) {
	data := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "null"},
		"transforms": []interface{}{
			map[string]interface{}{"type": "filter"},
			map[string]interface{}{"type": "validate"},
		},
	}
	imp, _ := DeriveImports(data)
	for _, w := range []string{
		module + "transform/filter", module + "transform/validate", module + "sink/null",
	} {
		if !has(imp, w) {
			t.Errorf("missing %s in %v", w, imp)
		}
	}
	if has(imp, module+"transform/lookup") {
		t.Error("undeclared lookup transform must not be linked")
	}
}

// A validate transform routing violations to its own reject sink must pull in
// that sink's package + driver, even when the main sink is something else.
func TestDeriveImports_TransformErrorSink(t *testing.T) {
	data := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "parquet"},
		"transforms": []interface{}{
			map[string]interface{}{"type": "validate",
				"errorSinkType":   "jdbc",
				"errorSinkConfig": map[string]interface{}{"driver": "postgres"}},
		},
	}
	imp, _ := DeriveImports(data)
	for _, w := range []string{module + "sink/jdbc", "github.com/lib/pq", module + "sink/parquet"} {
		if !has(imp, w) {
			t.Errorf("missing %s in %v", w, imp)
		}
	}
}

// A router transform routes rows to per-route sinks and an optional default
// sink; every one of those sink packages + drivers must be linked.
func TestDeriveImports_RouterRouteSinks(t *testing.T) {
	data := map[string]interface{}{
		"source": map[string]interface{}{"type": "csv"},
		"sink":   map[string]interface{}{"type": "null"},
		"transforms": []interface{}{
			map[string]interface{}{"type": "router",
				"routes": []interface{}{
					map[string]interface{}{"when": "amount > 1000", "sinkType": "parquet",
						"sinkConfig": map[string]interface{}{"path": "./hi.parquet"}},
					map[string]interface{}{"when": "region = 'EU'", "sinkType": "jdbc",
						"sinkConfig": map[string]interface{}{"driver": "postgres"}},
				},
				"defaultSinkType":   "jsonlines",
				"defaultSinkConfig": map[string]interface{}{"path": "./other.jsonl"}},
		},
	}
	imp, _ := DeriveImports(data)
	for _, w := range []string{
		module + "transform/router", module + "sink/parquet",
		module + "sink/jdbc", "github.com/lib/pq", module + "sink/jsonlines",
	} {
		if !has(imp, w) {
			t.Errorf("missing %s in %v", w, imp)
		}
	}
}

// ImportsForApp unions the app's declared imports with the derived component set.
func TestImportsForApp(t *testing.T) {
	app := []byte(`{
	  "imports": ["github.com/project-flogo/contrib/trigger/timer"],
	  "resources": [{"data": {
	    "source": {"type": "parquet"},
	    "sink": {"type": "jdbc", "driver": "postgres"}
	  }}]
	}`)
	imp, cgo, err := ImportsForApp(app)
	if err != nil {
		t.Fatal(err)
	}
	if cgo {
		t.Error("parquet->postgres should not require CGO")
	}
	for _, w := range []string{
		module + "source/parquet", module + "sink/jdbc", "github.com/lib/pq",
		"github.com/project-flogo/contrib/trigger/timer",
	} {
		if !has(imp, w) {
			t.Errorf("missing %s in %v", w, imp)
		}
	}
}
