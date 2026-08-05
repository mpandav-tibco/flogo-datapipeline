package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func readAll(t *testing.T, src source.Source, batchSize int) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for {
		batch, err := src.ReadBatch(batchSize)
		if batch != nil {
			for _, row := range batch.Rows {
				m := make(map[string]interface{}, len(batch.Columns))
				for i, c := range batch.Columns {
					m[c.Name] = row[i]
				}
				out = append(out, m)
			}
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
	}
}

func TestRestSource_SingleRequest_RootArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{
			{"id": 1, "name": "a"},
			{"id": 2, "name": "b"},
		})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{"url": srv.URL})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 10)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0]["name"] != "a" || rows[1]["name"] != "b" {
		t.Errorf("unexpected rows: %+v", rows)
	}
}

func TestRestSource_DataPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"meta": map[string]interface{}{"total": 2},
			"data": map[string]interface{}{
				"items": []map[string]interface{}{
					{"id": 1}, {"id": 2},
				},
			},
		})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{"url": srv.URL, "dataPath": "data.items"})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 10)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
}

func TestRestSource_PagePagination(t *testing.T) {
	pages := [][]map[string]interface{}{
		{{"id": 1}, {"id": 2}}, // page 1, full (pageSize=2) -> continue
		{{"id": 3}, {"id": 4}}, // page 2, full -> continue
		{{"id": 5}},            // page 3, short -> stop
	}
	var gotPages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := strconv.Atoi(r.URL.Query().Get("page"))
		gotPages = append(gotPages, r.URL.Query().Get("page")+"/"+r.URL.Query().Get("limit"))
		if p < 1 || p > len(pages) {
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{})
			return
		}
		_ = json.NewEncoder(w).Encode(pages[p-1])
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL,
		"pagination": map[string]interface{}{
			"type": "page", "pageSize": float64(2),
		},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 100)
	if len(rows) != 5 {
		t.Fatalf("got %d rows, want 5 (across 3 pages): %+v", len(rows), rows)
	}
	if len(gotPages) != 3 {
		t.Fatalf("fetched %d pages, want exactly 3 (stop on short page): %v", len(gotPages), gotPages)
	}
	if gotPages[0] != "1/2" || gotPages[1] != "2/2" || gotPages[2] != "3/2" {
		t.Errorf("unexpected page/limit params: %v", gotPages)
	}
}

func TestRestSource_OffsetPagination(t *testing.T) {
	all := []map[string]interface{}{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		end := off + lim
		if end > len(all) {
			end = len(all)
		}
		if off >= len(all) {
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{})
			return
		}
		_ = json.NewEncoder(w).Encode(all[off:end])
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL,
		"pagination": map[string]interface{}{
			"type": "offset", "pageSize": float64(2),
		},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 100)
	if len(rows) != 5 {
		t.Fatalf("got %d rows, want 5: %+v", len(rows), rows)
	}
}

func TestRestSource_CursorPagination_NextCursorPath(t *testing.T) {
	// Three "pages" chained by an opaque cursor value in the response.
	responses := map[string]map[string]interface{}{
		"": {
			"items":      []map[string]interface{}{{"id": 1}, {"id": 2}},
			"nextCursor": "c2",
		},
		"c2": {
			"items":      []map[string]interface{}{{"id": 3}},
			"nextCursor": "",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		_ = json.NewEncoder(w).Encode(responses[cursor])
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url":      srv.URL,
		"dataPath": "items",
		"pagination": map[string]interface{}{
			"type": "cursor", "nextCursorPath": "nextCursor",
		},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 100)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3: %+v", len(rows), rows)
	}
}

func TestRestSource_CursorPagination_NextURLPath(t *testing.T) {
	var callCount int
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/page1", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"items": []map[string]interface{}{{"id": 1}},
			"links": map[string]interface{}{"next": srv.URL + "/page2"},
		})
	})
	mux.HandleFunc("/page2", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"items": []map[string]interface{}{{"id": 2}},
			"links": map[string]interface{}{},
		})
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url":      srv.URL + "/page1",
		"dataPath": "items",
		"pagination": map[string]interface{}{
			"type": "cursor", "nextUrlPath": "links.next",
		},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 100)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(rows), rows)
	}
	if callCount != 2 {
		t.Fatalf("server was called %d times, want 2", callCount)
	}
}

func TestRestSource_Auth_Bearer(t *testing.T) {
	t.Setenv("REST_TEST_TOKEN", "s3cr3t")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{{"id": 1}})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL, "authType": "bearer", "bearerTokenEnv": "REST_TEST_TOKEN",
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()
	_ = readAll(t, src, 10)

	if gotAuth != "Bearer s3cr3t" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer s3cr3t")
	}
}

func TestRestSource_Auth_ApiKey(t *testing.T) {
	t.Setenv("REST_TEST_APIKEY", "k-123")
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Api-Key")
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL, "authType": "apikey", "apiKeyEnv": "REST_TEST_APIKEY",
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()
	_ = readAll(t, src, 10)

	if gotKey != "k-123" {
		t.Errorf("X-Api-Key header = %q, want %q", gotKey, "k-123")
	}
}

func TestRestSource_Auth_MissingEnv(t *testing.T) {
	_ = os.Unsetenv("REST_TEST_MISSING")
	_, err := source.New("rest", map[string]interface{}{
		"url": "http://example.invalid", "authType": "bearer", "bearerTokenEnv": "REST_TEST_MISSING",
	})
	if err == nil {
		t.Fatal("expected an error for a missing bearer token env var")
	}
}

func TestRestSource_RetryOn500ThenSucceed(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{{"id": 1}})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL, "maxRetries": float64(2), "retryBackoffMs": float64(1),
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	rows := readAll(t, src, 10)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 (after one retry): %+v", len(rows), rows)
	}
	if calls != 2 {
		t.Fatalf("server was called %d times, want 2 (1 failure + 1 retry)", calls)
	}
}

func TestRestSource_NonRetryable4xxFailsFast(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL, "maxRetries": float64(2), "retryBackoffMs": float64(1),
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	_, err = src.ReadBatch(10)
	if err == nil {
		t.Fatal("expected an error for a 404 response")
	}
	if calls != 1 {
		t.Fatalf("server was called %d times, want 1 (404 is not retried)", calls)
	}
}

func TestRestSource_ExplicitColumns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{{"id": 1, "amount": 12.5, "extra": "ignored-by-schema"}})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url": srv.URL,
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "amount", "type": "float"},
		},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	if got := src.Schema(); len(got) != 2 || got[0].Type != record.TypeInt || got[1].Type != record.TypeFloat {
		t.Fatalf("Schema() = %+v, want explicit int/float columns", got)
	}

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("got %d rows, want 1", batch.Len())
	}
}

func TestRestSource_SkipDiscardsAcrossPages(t *testing.T) {
	pages := [][]map[string]interface{}{
		{{"id": 1}, {"id": 2}},
		{{"id": 3}, {"id": 4}},
		{{"id": 5}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if p < 1 || p > len(pages) {
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{})
			return
		}
		_ = json.NewEncoder(w).Encode(pages[p-1])
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{
		"url":        srv.URL,
		"pagination": map[string]interface{}{"type": "page", "pageSize": float64(2)},
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	if err := src.Skip(3); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	rows := readAll(t, src, 100)
	if len(rows) != 2 {
		t.Fatalf("got %d rows after skipping 3 of 5, want 2: %+v", len(rows), rows)
	}
	if fmt.Sprint(rows[0]["id"]) != "4" || fmt.Sprint(rows[1]["id"]) != "5" {
		t.Errorf("unexpected rows after skip: %+v", rows)
	}
}

func TestRestSource_MissingURL(t *testing.T) {
	if _, err := source.New("rest", map[string]interface{}{}); err == nil {
		t.Fatal("expected an error when 'url' is missing")
	}
}

func TestRestSource_DataPathNotArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": "not-an-array"})
	}))
	defer srv.Close()

	src, err := source.New("rest", map[string]interface{}{"url": srv.URL, "dataPath": "data"})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	if _, err := src.ReadBatch(10); err == nil {
		t.Fatal("expected an error when dataPath does not resolve to an array")
	}
}
