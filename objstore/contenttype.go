package objstore

import "strings"

// ContentTypeForKey returns the MIME content type an object should be stored
// with, derived from its key's file extension. Object-store backends default a
// missing content type to "application/octet-stream", which browsers and the
// S3/MinIO console treat as an opaque binary (download-only, no inline preview).
// Setting a specific type lets NDJSON/CSV/JSON exports preview inline and be
// recognized correctly by downstream tools (e.g. OpenSearch ingestion).
func ContentTypeForKey(key string) string {
	k := strings.ToLower(key)
	switch {
	case strings.HasSuffix(k, ".ndjson"), strings.HasSuffix(k, ".jsonl"):
		return "application/x-ndjson"
	case strings.HasSuffix(k, ".json"):
		return "application/json"
	case strings.HasSuffix(k, ".csv"):
		return "text/csv; charset=utf-8"
	case strings.HasSuffix(k, ".tsv"):
		return "text/tab-separated-values; charset=utf-8"
	case strings.HasSuffix(k, ".parquet"):
		return "application/vnd.apache.parquet"
	case strings.HasSuffix(k, ".txt"):
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}
