// Package build derives the set of Go packages a datapipeline app must link.
//
// It is the datapipeline analogue of the Flogo flow build model: instead of a
// monolithic binary that registers every source, sink, transform and driver, a
// binary is compiled with ONLY the components its app actually references. Given
// a pipeline resource's "data" object (or a whole .datapipeline app document),
// DeriveImports / ImportsForApp return the exact blank-import set — no unused
// JDBC drivers, Kafka client, or cloud SDKs, and CGO only when a CGO-linked
// driver (sqlite) is used.
//
// This lets `cmd/dp-build` (and any future app generator or SDK path) produce
// lean, per-app binaries automatically, the same way `flogo build` does for
// Flogo flows.
package build

import (
	"encoding/json"
	"sort"
	"strings"
)

// module is the import-path prefix for this repository's component packages.
const module = "github.com/mpandav-tibco/flogo-datapipeline/"

// Component-type -> package maps. Relative paths are joined to module; paths
// that already contain a dot (external drivers) are used verbatim.
var (
	sourcePkg = map[string]string{
		"csv": "source/csv", "jsonlines": "source/jsonlines", "parquet": "source/parquet",
		"avro": "source/avro",
		"jdbc": "source/jdbc", "kafka": "source/kafka", "fixedwidth": "source/fixedwidth",
		"xml": "source/xml", "sectioned": "source/sectioned",
		"s3": "source/objstore", "gcs": "source/objstore", "adls": "source/objstore",
	}
	sinkPkg = map[string]string{
		"null": "sink/null", "parquet": "sink/parquet", "jdbc": "sink/jdbc", "kafka": "sink/kafka",
		"avro": "sink/avro",
		"csv":  "sink/csv", "jsonlines": "sink/jsonlines", "template": "sink/template", "dlq": "sink/dlq",
		"s3-sink": "sink/objstore", "gcs-sink": "sink/objstore", "adls-sink": "sink/objstore",
		"s3": "sink/objstore", "gcs": "sink/objstore", "adls": "sink/objstore",
	}
	transformPkg = map[string]string{
		"filter": "transform/filter", "map": "transform/mapper", "mapper": "transform/mapper",
		"validate": "transform/validate", "schemadrift": "transform/schemadrift",
		"dq": "transform/dq", "hwm": "transform/hwm", "lookup": "transform/lookup",
		"cast": "transform/cast", "explode": "transform/explode", "flatten": "transform/flatten",
		"router": "transform/router", "split": "transform/router",
	}
	objstoreProvider = map[string]string{
		"s3": "objstore/s3", "gcs": "objstore/gcs", "adls": "objstore/adls",
	}
	driverPkg = map[string]string{
		"postgres": "github.com/lib/pq", "postgresql": "github.com/lib/pq",
		"mysql":  "github.com/go-sql-driver/mysql",
		"sqlite": "github.com/mattn/go-sqlite3", "sqlite3": "github.com/mattn/go-sqlite3",
	}
	cgoDrivers     = map[string]bool{"sqlite": true, "sqlite3": true}
	checkpointPkg  = map[string]string{"file": "checkpoint/file", "redis": "checkpoint/redis"}
	watermarkPkg   = map[string]string{"file": "watermark/file", "redis": "watermark/redis", "postgres": "watermark/postgres"}
	metricsPkg     = map[string]string{"prometheus": "metrics/prometheus", "otel": "metrics/otel"}
	coordinatorPkg = map[string]string{"redis": "checkpoint/redis", "postgres": "watermark/postgres"}
)

type collector struct {
	set map[string]bool
	cgo bool
}

func (c *collector) add(rel string) {
	if rel == "" {
		return
	}
	if strings.Contains(rel, ".") { // external package (already fully qualified)
		c.set[rel] = true
	} else {
		c.set[module+rel] = true
	}
}

func (c *collector) driver(d string) {
	if p, ok := driverPkg[strings.ToLower(d)]; ok {
		c.add(p)
	}
	if cgoDrivers[strings.ToLower(d)] {
		c.cgo = true
	}
}

func str(m map[string]interface{}, k string) string { v, _ := m[k].(string); return v }
func obj(v interface{}) map[string]interface{}      { m, _ := v.(map[string]interface{}); return m }

// driverOf reads the JDBC driver name from either the flattened top level or a
// nested "options" object.
func driverOf(m map[string]interface{}) string {
	if d := str(m, "driver"); d != "" {
		return d
	}
	return str(obj(m["options"]), "driver")
}

// formatOf reads the inner format for an object-store source/sink from either
// the flattened top level or a nested "options" object.
func formatOf(m map[string]interface{}) string {
	if f := str(m, "format"); f != "" {
		return f
	}
	return str(obj(m["options"]), "format")
}

func (c *collector) sourceOf(src map[string]interface{}) {
	if src == nil {
		return
	}
	t := str(src, "type")
	c.add(sourcePkg[t])
	if prov, ok := objstoreProvider[t]; ok {
		c.add(prov)
		c.add(sourcePkg[formatOf(src)]) // inner format reader (csv/jsonlines/parquet)
	}
	if t == "jdbc" {
		c.driver(driverOf(src))
	}
	c.sinkLike(obj(src["errorSink"])) // streaming source DLQ
}

func (c *collector) sinkOf(snk map[string]interface{}) {
	if snk == nil {
		return
	}
	t := str(snk, "type")
	c.add(sinkPkg[t])
	if t == "s3-sink" || t == "gcs-sink" || t == "adls-sink" ||
		t == "s3" || t == "gcs" || t == "adls" {
		prov := strings.SplitN(t, "-", 2)[0]
		c.add(objstoreProvider[prov])
		c.add(sinkPkg[formatOf(snk)]) // inner format writer
	}
	if t == "jdbc" {
		c.driver(driverOf(snk))
	}
}

// sinkLike handles inner sink references (DLQ block, source errorSink), which
// may name a sink type directly or wrap one via innerType.
func (c *collector) sinkLike(m map[string]interface{}) {
	if m == nil {
		return
	}
	if t := str(m, "type"); t != "" {
		c.sinkOf(m)
	}
	inner := str(m, "innerType")
	if inner == "" {
		inner = str(obj(m["options"]), "innerType")
	}
	if inner != "" {
		c.add(sinkPkg[inner])
		if inner == "jdbc" {
			c.driver(driverOf(obj(m["options"])))
		}
	}
}

// DeriveImports inspects a single pipeline resource's "data" object and returns
// the sorted Go import paths a lean binary must blank-import to run it, plus
// whether the build needs CGO (true only for CGO-linked drivers like sqlite).
// The pipeline action and the timer trigger are always included so the emitted
// import set is complete on its own.
func DeriveImports(data map[string]interface{}) (imports []string, needsCGO bool) {
	c := &collector{set: map[string]bool{}}
	c.add("action/pipeline")
	c.set["github.com/project-flogo/contrib/trigger/timer"] = true

	c.sourceOf(obj(data["source"]))
	c.sinkOf(obj(data["sink"]))

	if ts, ok := data["transforms"].([]interface{}); ok {
		for _, t := range ts {
			tm := obj(t)
			c.add(transformPkg[str(tm, "type")])
			// A transform (e.g. validate routing violations, dq routing a report)
			// may write to its OWN sink; link that sink's package + driver/provider.
			for _, key := range []string{"errorSinkType", "reportSinkType"} {
				if est := str(tm, key); est != "" {
					c.sinkOf(map[string]interface{}{
						"type": est, "options": tm[strings.Replace(key, "Type", "Config", 1)]})
				}
			}
			// The router transform routes rows to per-route sinks and an optional
			// default sink; link each of those sink packages + driver/provider.
			if str(tm, "type") == "router" || str(tm, "type") == "split" {
				if routes, ok := tm["routes"].([]interface{}); ok {
					for _, rr := range routes {
						rm := obj(rr)
						if st := str(rm, "sinkType"); st != "" {
							c.sinkOf(map[string]interface{}{"type": st, "options": rm["sinkConfig"]})
						}
					}
				}
				if st := str(tm, "defaultSinkType"); st != "" {
					c.sinkOf(map[string]interface{}{"type": st, "options": tm["defaultSinkConfig"]})
				}
			}
		}
	}
	if str(obj(data["schemaDrift"]), "policy") != "" {
		c.add("transform/schemadrift")
	}
	if cp := obj(data["checkpoint"]); cp != nil {
		if t := str(cp, "type"); t != "" && t != "none" {
			c.add(checkpointPkg[t])
		}
	}
	if inc := obj(data["incremental"]); str(inc, "mode") == "hwm" {
		c.add("transform/hwm")
		st := str(obj(inc["watermark"]), "storeType")
		if st == "" {
			st = str(inc, "storeType")
		}
		if st == "" {
			st = "file"
		}
		c.add(watermarkPkg[st])
	}
	c.add(metricsPkg[str(obj(data["observability"]), "metrics")])
	c.sinkLike(obj(data["dlq"]))
	if sc := obj(data["scaling"]); str(sc, "mode") == "distributed" {
		c.add(coordinatorPkg[str(obj(sc["coordinator"]), "type")])
	}

	for k := range c.set {
		imports = append(imports, k)
	}
	sort.Strings(imports)
	return imports, c.cgo
}

// ImportsForApp parses a whole .datapipeline (flogo:app) document and returns
// the union of its declared app-level imports and the derived component imports
// for every pipeline resource it contains, plus whether CGO is required.
func ImportsForApp(appJSON []byte) (imports []string, needsCGO bool, err error) {
	var app struct {
		Imports   []string `json:"imports"`
		Resources []struct {
			Data map[string]interface{} `json:"data"`
		} `json:"resources"`
	}
	if err = json.Unmarshal(appJSON, &app); err != nil {
		return nil, false, err
	}
	set := map[string]bool{}
	for _, i := range app.Imports { // declared imports (trigger, action, functions…)
		set[i] = true
	}
	for _, r := range app.Resources {
		di, cgo := DeriveImports(r.Data)
		needsCGO = needsCGO || cgo
		for _, i := range di {
			set[i] = true
		}
	}
	for k := range set {
		imports = append(imports, k)
	}
	sort.Strings(imports)
	return imports, needsCGO, nil
}
