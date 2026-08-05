package main

import (
	"testing"
)

const validPipelineJSON = `{
  "resources": [
    {
      "id": "pipeline:demo",
      "data": {
        "name": "demo",
        "source": {"type": "csv", "options": {"path": "/tmp/a.csv"}},
        "sink": {"type": "null"}
      }
    }
  ]
}`

const invalidPipelineJSON = `{
  "resources": [
    {
      "id": "pipeline:broken",
      "data": {
        "name": "broken",
        "source": {"type": ""},
        "sink": {"type": "null"}
      }
    }
  ]
}`

func TestRunValidate_ValidPipeline(t *testing.T) {
	if code := runValidate("test.datapipeline", validPipelineJSON); code != 0 {
		t.Errorf("exit code = %d, want 0 for a valid pipeline definition", code)
	}
}

func TestRunValidate_InvalidPipeline(t *testing.T) {
	if code := runValidate("test.datapipeline", invalidPipelineJSON); code != 1 {
		t.Errorf("exit code = %d, want 1 for an invalid pipeline definition (empty source.type)", code)
	}
}

func TestRunValidate_MalformedJSON(t *testing.T) {
	if code := runValidate("test.datapipeline", `{not json`); code != 1 {
		t.Errorf("exit code = %d, want 1 for malformed app JSON", code)
	}
}

func TestRunValidate_NoPipelineResources(t *testing.T) {
	if code := runValidate("test.datapipeline", `{"resources": [{"id": "trigger:timer", "data": {}}]}`); code != 1 {
		t.Errorf("exit code = %d, want 1 when no pipeline: resources are present", code)
	}
}

func TestRunValidate_MultiplePipelines_OneInvalidFailsAll(t *testing.T) {
	doc := `{
      "resources": [
        {"id": "pipeline:good", "data": {"name":"good","source":{"type":"csv","options":{"path":"/tmp/a.csv"}},"sink":{"type":"null"}}},
        {"id": "pipeline:bad", "data": {"name":"bad","source":{"type":""},"sink":{"type":"null"}}}
      ]
    }`
	if code := runValidate("test.datapipeline", doc); code != 1 {
		t.Errorf("exit code = %d, want 1 when any pipeline in the app is invalid", code)
	}
}
