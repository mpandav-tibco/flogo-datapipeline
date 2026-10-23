// Package gcs registers a Google Cloud Storage backend with the objstore registry.
// Import with a blank import to activate:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/gcs"
//
// Config keys:
//
//	bucket                string  REQUIRED — GCS bucket name
//	credentialsFileEnv    string  env-var name that holds the path to a service-account
//	                               JSON file (GOOGLE_APPLICATION_CREDENTIALS style).
//	                               If empty, Application Default Credentials are used
//	                               (Workload Identity on GKE, gcloud auth on dev machines).
//	project               string  GCP project ID (required only when listing all buckets;
//	                               not needed for single-bucket operations).
//	endpoint              string  override the API endpoint, e.g. for the fake-gcs-server
//	                               emulator: "http://127.0.0.1:4443/storage/v1/".
//	                               When set without credentials, auth is disabled.
//
// SECURITY: never embed service-account JSON or literal credential paths in
// pipeline resource files.  Use credentialsFileEnv and bind the env var from
// a Kubernetes Secret or a Flogo $property.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	objstore.Register("gcs", newGCSStore)
}

type gcsStore struct {
	client *storage.Client
	bucket string
}

func newGCSStore(cfg map[string]interface{}) (objstore.Store, error) {
	bucket, _ := cfg["bucket"].(string)
	if bucket == "" {
		return nil, fmt.Errorf("gcs store: 'bucket' is required")
	}

	credEnv, _ := cfg["credentialsFileEnv"].(string)
	// Inline service-account JSON (from a Studio connection).
	inlineCredJSON, _ := cfg["credentialsJson"].(string)
	// Optional custom endpoint (e.g. the fake-gcs-server emulator for local dev).
	endpoint, _ := cfg["endpoint"].(string)

	var opts []option.ClientOption
	if credEnv != "" {
		credFile := os.Getenv(credEnv)
		if credFile == "" {
			return nil, fmt.Errorf("gcs store: env var %q (credentialsFileEnv) is empty", credEnv)
		}
		opts = append(opts, option.WithCredentialsFile(credFile))
	} else if inlineCredJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(inlineCredJSON)))
	}
	// If no credentials option provided, storage.NewClient uses ADC automatically.

	// A custom endpoint points the client at an emulator (fake-gcs-server) or a
	// private-service-connect URL. Emulators are unauthenticated, so when an
	// endpoint is set without explicit credentials we disable auth.
	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
		if credEnv == "" && inlineCredJSON == "" {
			opts = append(opts, option.WithoutAuthentication())
		}
	}

	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs store: create client: %w", err)
	}

	return &gcsStore{client: client, bucket: bucket}, nil
}

// List returns all objects whose key matches the given prefix or glob pattern.
// GCS Objects.list does not support glob syntax; we strip the wildcard suffix,
// call the API with the literal prefix, then filter with filepath.Match.
func (s *gcsStore) List(ctx context.Context, prefix string) ([]objstore.Object, error) {
	glob := ""
	listPrefix := prefix
	if strings.Contains(prefix, "*") {
		starIdx := strings.Index(prefix, "*")
		slashIdx := strings.LastIndex(prefix[:starIdx], "/")
		if slashIdx >= 0 {
			listPrefix = prefix[:slashIdx+1]
		} else {
			listPrefix = ""
		}
		glob = prefix
	}

	bkt := s.client.Bucket(s.bucket)
	query := &storage.Query{Prefix: listPrefix}

	var result []objstore.Object
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs store: list %q: %w", prefix, err)
		}
		key := attrs.Name
		if glob != "" {
			matched, err := filepath.Match(glob, key)
			if err != nil {
				return nil, fmt.Errorf("gcs store: bad glob %q: %w", glob, err)
			}
			if !matched {
				continue
			}
		}
		result = append(result, objstore.Object{
			Key:  key,
			Size: attrs.Size,
			ETag: attrs.Etag,
		})
	}
	return result, nil
}

// GetReader returns a streaming reader backed by GCS Object.NewReader.
// The caller must Close the reader when done.
func (s *gcsStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs store: get %q: %w", key, err)
	}
	return r, nil
}

// GetRange returns a reader over [offset, offset+length) via a GCS ranged read,
// so Parquet reads fetch only the footer and the selected column chunks instead
// of the whole object.
func (s *gcsStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, fmt.Errorf("gcs store: get range %q [%d,%d): %w", key, offset, offset+length, err)
	}
	return r, nil
}

// GetRangeIfMatch implements objstore.VersionedRangeReader. The read is pinned
// to the generation whose ETag matches; gzip-encoded objects are refused
// because GCS serves them decompressed and ignores the byte range.
func (s *gcsStore) GetRangeIfMatch(ctx context.Context, key, etag string, offset, length int64) (io.ReadCloser, error) {
	obj := s.client.Bucket(s.bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("gcs store: %q: %w", key, objstore.ErrObjectChanged)
		}
		return nil, fmt.Errorf("gcs store: attrs %q: %w", key, err)
	}
	if attrs.Etag != etag || attrs.ContentEncoding == "gzip" {
		return nil, fmt.Errorf("gcs store: %q: %w", key, objstore.ErrObjectChanged)
	}
	r, err := obj.If(storage.Conditions{GenerationMatch: attrs.Generation}).NewRangeReader(ctx, offset, length)
	if err != nil {
		var ge *googleapi.Error
		if errors.Is(err, storage.ErrObjectNotExist) || (errors.As(err, &ge) && ge.Code == http.StatusPreconditionFailed) {
			return nil, fmt.Errorf("gcs store: %q: %w", key, objstore.ErrObjectChanged)
		}
		return nil, fmt.Errorf("gcs store: get range %q [%d,%d): %w", key, offset, offset+length, err)
	}
	return r, nil
}

// PutWriter opens a GCS resumable upload using storage.NewWriter.
// storage.NewWriter is an io.WriteCloser; calling Close commits the upload.
// We wrap it in gcsWriter so that Close returns the underlying error.
func (s *gcsStore) PutWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	wc := s.client.Bucket(s.bucket).Object(key).NewWriter(ctx)
	wc.ContentType = objstore.ContentTypeForKey(key)
	// ChunkSize = 0 disables chunking and sends a single non-resumable upload for
	// small objects (< 5 MB).  For larger objects GCS automatically uses resumable
	// uploads when ChunkSize > 0.  Set a sensible default.
	wc.ChunkSize = 8 * 1024 * 1024 // 8 MB chunks
	return wc, nil
}

// Delete removes the object.  If the key does not exist GCS returns a 404;
// we treat that as a no-op per the Store contract.
func (s *gcsStore) Delete(ctx context.Context, key string) error {
	err := s.client.Bucket(s.bucket).Object(key).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gcs store: delete %q: %w", key, err)
	}
	return nil
}

// Close releases the underlying HTTP transport.
func (s *gcsStore) Close() error {
	return s.client.Close()
}
