package main

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// The blank imports in main.go are what register each source/sink format into
// the runtime registry. A dropped or accidentally commented-out blank import
// compiles cleanly and passes every package unit test (those import the format
// package directly), yet silently makes the format unavailable in the shipped
// binary. These guards assert the binary actually links every format whose
// registration is unconditional, so that class of regression fails loudly here.

func toSet(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func TestBinaryRegistersSources(t *testing.T) {
	got := toSet(source.Formats())
	for _, format := range []string{
		"csv", "tsv", "jdbc",
		"avro", "fixedwidth", "flatfile", "jsonlines", "ndjson",
		"parquet", "sectioned", "xml",
		"kafka", "podlog",
	} {
		if !got[format] {
			t.Errorf("source format %q is not registered in the binary — is source/%s blank-imported in main.go?", format, format)
		}
	}
}

func TestBinaryRegistersSinks(t *testing.T) {
	got := toSet(sink.Types())
	for _, typ := range []string{
		"jdbc", "null", "discard",
		"avro", "parquet",
		"csv", "tsv", "jsonlines", "ndjson", "template",
		"dlq", "kafka",
	} {
		if !got[typ] {
			t.Errorf("sink type %q is not registered in the binary — is its sink package blank-imported in main.go?", typ)
		}
	}
}
