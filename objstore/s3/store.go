// Package s3 registers an AWS S3 (and S3-compatible) object-storage backend
// with the objstore registry.  Import with a blank import to activate:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"
//
// Config keys (all optional unless noted):
//
//	bucket           string  REQUIRED — S3 bucket name
//	region           string  AWS region, e.g. "us-east-1"
//	accessKeyIdEnv   string  env-var name that holds the access key ID
//	secretAccessKeyEnv string env-var name that holds the secret access key
//	sessionTokenEnv  string  env-var name that holds a temporary session token
//	endpoint         string  custom endpoint URL for MinIO / LocalStack
//	forcePathStyle   bool    use path-style addressing (required for MinIO)
//
// Credential resolution order:
//  1. Static credentials from env vars named by accessKeyIdEnv / secretAccessKeyEnv.
//  2. SDK default credential chain (env vars, ~/.aws/credentials, IRSA, EC2 instance profile).
//
// SECURITY: never put literal accessKeyId / secretAccessKey in pipeline JSON.
// Use Flogo $property references backed by Kubernetes Secrets and set the
// *Env config keys to point to the env var that Flogo injects at runtime.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

func init() {
	objstore.Register("s3", newS3Store)
}

type s3Store struct {
	client     *s3.Client
	bucket     string
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

func newS3Store(cfg map[string]interface{}) (objstore.Store, error) {
	bucket, _ := cfg["bucket"].(string)
	if bucket == "" {
		return nil, fmt.Errorf("s3 store: 'bucket' is required")
	}
	region, _ := cfg["region"].(string)
	if region == "" {
		region = "us-east-1"
	}

	endpoint, _ := cfg["endpoint"].(string)
	forcePathStyle, _ := cfg["forcePathStyle"].(bool)

	// Resolve credentials: prefer env-var-referenced secrets, then inline
	// accessKeyId/secretAccessKey (used by Studio connections and local/dev),
	// else fall back to the SDK default chain.
	accessKeyIDEnv, _ := cfg["accessKeyIdEnv"].(string)
	secretKeyEnv, _ := cfg["secretAccessKeyEnv"].(string)
	sessionTokenEnv, _ := cfg["sessionTokenEnv"].(string)

	// Inline credential values (from a Studio connection or explicit config).
	inlineKeyID, _ := cfg["accessKeyId"].(string)
	inlineSecret, _ := cfg["secretAccessKey"].(string)
	inlineToken, _ := cfg["sessionToken"].(string)

	var awsCfg aws.Config
	var err error

	if accessKeyIDEnv != "" && secretKeyEnv != "" {
		keyID := os.Getenv(accessKeyIDEnv)
		secret := os.Getenv(secretKeyEnv)
		token := ""
		if sessionTokenEnv != "" {
			token = os.Getenv(sessionTokenEnv)
		}
		if keyID == "" {
			return nil, fmt.Errorf("s3 store: env var %q (accessKeyIdEnv) is empty", accessKeyIDEnv)
		}
		if secret == "" {
			return nil, fmt.Errorf("s3 store: env var %q (secretAccessKeyEnv) is empty", secretKeyEnv)
		}
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(region),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(keyID, secret, token),
			),
		)
	} else if inlineKeyID != "" && inlineSecret != "" {
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(region),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(inlineKeyID, inlineSecret, inlineToken),
			),
		)
	} else {
		// Fall back to SDK default chain (env vars, ~/.aws, IRSA, instance profile).
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(region),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("s3 store: load AWS config: %w", err)
	}

	// Build client options — add a custom endpoint resolver if endpoint is set.
	clientOpts := []func(*s3.Options){}
	if endpoint != "" {
		ep := endpoint // capture for closure
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = forcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &s3Store{
		client:     client,
		bucket:     bucket,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
	}, nil
}

// List returns all objects whose key matches the given prefix or glob pattern.
// S3 ListObjectsV2 does not support glob syntax; we strip the wildcard suffix,
// call ListObjectsV2 with the literal prefix, then filter with filepath.Match.
// Pagination is handled automatically to avoid truncation on large buckets.
func (s *s3Store) List(ctx context.Context, prefix string) ([]objstore.Object, error) {
	// Separate the literal prefix from the glob pattern.
	glob := ""
	listPrefix := prefix
	if strings.Contains(prefix, "*") {
		// Find the last directory boundary before the first wildcard.
		starIdx := strings.Index(prefix, "*")
		slashIdx := strings.LastIndex(prefix[:starIdx], "/")
		if slashIdx >= 0 {
			listPrefix = prefix[:slashIdx+1]
		} else {
			listPrefix = ""
		}
		glob = prefix
	}

	var result []objstore.Object
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(listPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 store: list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if glob != "" {
				// filepath.Match uses '/' as a separator on all platforms.
				matched, err := filepath.Match(glob, key)
				if err != nil {
					return nil, fmt.Errorf("s3 store: bad glob %q: %w", glob, err)
				}
				if !matched {
					continue
				}
			}
			etag := ""
			if obj.ETag != nil {
				etag = strings.Trim(*obj.ETag, `"`)
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
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

// GetReader returns a streaming reader backed by S3 GetObject.
// The stream is NOT buffered to disk; formats that require random access (e.g.
// Parquet) must set download:true in the source config to spool to a temp file.
func (s *s3Store) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 store: get %q: %w", key, err)
	}
	return out.Body, nil
}

// GetRange returns a reader over [offset, offset+length) via an S3 ranged GET,
// so Parquet reads fetch only the footer and the selected column chunks instead
// of the whole object.
func (s *s3Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rng := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 store: get range %q %s: %w", key, rng, err)
	}
	return out.Body, nil
}

// PutWriter implements multipart upload via an io.Pipe.
// The upload is committed when the caller calls Close on the returned writer.
// If the process crashes mid-upload, S3 retains incomplete parts — configure
// an S3 lifecycle rule to abort incomplete multipart uploads after 24 h.
func (s *s3Store) PutWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	uploadDone := make(chan error, 1)

	go func() {
		_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        pr,
			ContentType: aws.String(objstore.ContentTypeForKey(key)),
		})
		// Signal the read end so it unblocks even on error.
		pr.CloseWithError(err)
		uploadDone <- err
	}()

	return &s3Writer{pw: pw, done: uploadDone}, nil
}

// s3Writer wraps the write end of the pipe and waits for the upload goroutine
// to finish on Close so that errors from the SDK are not silently swallowed.
type s3Writer struct {
	pw   *io.PipeWriter
	done chan error
}

func (w *s3Writer) Write(p []byte) (int, error) { return w.pw.Write(p) }

func (w *s3Writer) Close() error {
	// Close the write end of the pipe — signals EOF to the upload goroutine.
	if err := w.pw.Close(); err != nil {
		return err
	}
	// Wait for the upload goroutine to complete and surface any SDK error.
	return <-w.done
}

// Delete removes the object.  AWS S3 returns success even if the key does not
// exist, so no special handling is needed.
func (s *s3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Ignore NoSuchKey — treat it as a no-op per the Store contract.
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil
		}
		return fmt.Errorf("s3 store: delete %q: %w", key, err)
	}
	return nil
}

// Close is a no-op for S3 — the AWS SDK manages HTTP connection pools
// independently of any per-client state.
func (s *s3Store) Close() error { return nil }
