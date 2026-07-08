// Package adls registers an Azure Data Lake Storage Gen2 (ADLS Gen2 / Blob
// Storage) backend with the objstore registry.
// Import with a blank import to activate:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/adls"
//
// Config keys:
//
//	container             string  REQUIRED — blob container name
//	accountName           string  REQUIRED — storage account name
//	sasTokenEnv           string  env-var name that holds a SAS token
//	                               (preferred; grants scoped access per container)
//	accountKeyEnv         string  env-var name that holds the storage account key
//	                               (used when sasTokenEnv is empty)
//	serviceURL            string  override the endpoint, e.g. for Azurite emulator:
//	                               "http://127.0.0.1:10000/<account>"
//
// SECURITY: never embed SAS tokens or account keys in pipeline JSON.
// Use sasTokenEnv / accountKeyEnv and bind the env vars from Azure Key Vault
// references or Kubernetes Secrets via Flogo $property mappings.
package adls

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

func init() {
	objstore.Register("adls", newADLSStore)
}

type adlsStore struct {
	client    *azblob.Client
	container string
}

func newADLSStore(cfg map[string]interface{}) (objstore.Store, error) {
	container, _ := cfg["container"].(string)
	if container == "" {
		return nil, fmt.Errorf("adls store: 'container' is required")
	}
	accountName, _ := cfg["accountName"].(string)
	if accountName == "" {
		return nil, fmt.Errorf("adls store: 'accountName' is required")
	}

	serviceURL, _ := cfg["serviceURL"].(string)
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	}

	sasTokenEnv, _ := cfg["sasTokenEnv"].(string)
	accountKeyEnv, _ := cfg["accountKeyEnv"].(string)

	var client *azblob.Client
	var err error

	switch {
	case sasTokenEnv != "":
		sasToken := os.Getenv(sasTokenEnv)
		if sasToken == "" {
			return nil, fmt.Errorf("adls store: env var %q (sasTokenEnv) is empty", sasTokenEnv)
		}
		// A SAS token can be embedded directly in the service URL or supplied as a
		// query string.  azblob.NewClientWithNoCredential handles both forms.
		urlWithSAS := serviceURL
		if !strings.Contains(urlWithSAS, "?") {
			urlWithSAS = urlWithSAS + "?" + strings.TrimPrefix(sasToken, "?")
		}
		client, err = azblob.NewClientWithNoCredential(urlWithSAS, nil)

	case accountKeyEnv != "":
		accountKey := os.Getenv(accountKeyEnv)
		if accountKey == "" {
			return nil, fmt.Errorf("adls store: env var %q (accountKeyEnv) is empty", accountKeyEnv)
		}
		cred, credErr := azblob.NewSharedKeyCredential(accountName, accountKey)
		if credErr != nil {
			return nil, fmt.Errorf("adls store: build shared-key credential: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)

	default:
		return nil, fmt.Errorf("adls store: provide either 'sasTokenEnv' or 'accountKeyEnv'")
	}

	if err != nil {
		return nil, fmt.Errorf("adls store: create client: %w", err)
	}

	return &adlsStore{client: client, container: container}, nil
}

// List returns all blobs whose name matches the given prefix or glob pattern.
// Azure ListBlobs does not support glob syntax; we strip the wildcard suffix,
// call the API with the literal prefix, then filter with filepath.Match.
func (s *adlsStore) List(ctx context.Context, prefix string) ([]objstore.Object, error) {
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

	containerClient := s.client.ServiceClient().NewContainerClient(s.container)

	var result []objstore.Object
	pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: strPtr(listPrefix),
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("adls store: list %q: %w", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			key := *item.Name
			if glob != "" {
				matched, err := filepath.Match(glob, key)
				if err != nil {
					return nil, fmt.Errorf("adls store: bad glob %q: %w", glob, err)
				}
				if !matched {
					continue
				}
			}
			var size int64
			var etag string
			if item.Properties != nil {
				if item.Properties.ContentLength != nil {
					size = *item.Properties.ContentLength
				}
				if item.Properties.ETag != nil {
					etag = string(*item.Properties.ETag)
				}
			}
			result = append(result, objstore.Object{
				Key:  key,
				Size: size,
				ETag: etag,
			})
		}
	}
	return result, nil
}

// GetReader returns a streaming reader backed by DownloadStream.
func (s *adlsStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	blobClient := s.client.ServiceClient().
		NewContainerClient(s.container).
		NewBlobClient(key)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("adls store: get %q: %w", key, err)
	}
	return resp.Body, nil
}

// PutWriter implements a streaming upload using an io.Pipe fed into
// blockblob.UploadStream, which automatically selects block size and
// parallelism.  The upload is committed when Close is called.
func (s *adlsStore) PutWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	bbClient := s.client.ServiceClient().
		NewContainerClient(s.container).
		NewBlockBlobClient(key)

	pr, pw := io.Pipe()
	uploadDone := make(chan error, 1)

	go func() {
		_, err := bbClient.UploadStream(ctx, pr, &blockblob.UploadStreamOptions{
			BlockSize:   4 * 1024 * 1024, // 4 MB blocks
			Concurrency: 4,
		})
		pr.CloseWithError(err)
		uploadDone <- err
	}()

	return &adlsWriter{pw: pw, done: uploadDone}, nil
}

// adlsWriter wraps the write end of the pipe and surfaces upload errors on Close.
type adlsWriter struct {
	pw   *io.PipeWriter
	done chan error
}

func (w *adlsWriter) Write(p []byte) (int, error) { return w.pw.Write(p) }

func (w *adlsWriter) Close() error {
	if err := w.pw.Close(); err != nil {
		return err
	}
	return <-w.done
}

// Delete removes the blob.  Returns nil if the blob does not exist.
func (s *adlsStore) Delete(ctx context.Context, key string) error {
	blobClient := s.client.ServiceClient().
		NewContainerClient(s.container).
		NewBlobClient(key)

	_, err := blobClient.Delete(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil
		}
		return fmt.Errorf("adls store: delete %q: %w", key, err)
	}
	return nil
}

// Close is a no-op — the Azure SDK manages connection pools internally.
func (s *adlsStore) Close() error { return nil }

func strPtr(s string) *string { return &s }
