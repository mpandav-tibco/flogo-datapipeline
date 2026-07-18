package objstore

import "testing"

func TestContentTypeForKey(t *testing.T) {
	cases := map[string]string{
		"ns/pod/2026/07/18/part-0001.ndjson": "application/x-ndjson",
		"x.jsonl":                            "application/x-ndjson",
		"UPPER.NDJSON":                       "application/x-ndjson",
		"logs/2026/data.json":                "application/json",
		"part-0001.ndjson.manifest.json":     "application/json",
		"export.csv":                         "text/csv; charset=utf-8",
		"export.tsv":                         "text/tab-separated-values; charset=utf-8",
		"cold/part.parquet":                  "application/vnd.apache.parquet",
		"notes.txt":                          "text/plain; charset=utf-8",
		"blob.bin":                           "application/octet-stream",
		"noextension":                        "application/octet-stream",
	}
	for key, want := range cases {
		if got := ContentTypeForKey(key); got != want {
			t.Errorf("ContentTypeForKey(%q) = %q, want %q", key, got, want)
		}
	}
}
