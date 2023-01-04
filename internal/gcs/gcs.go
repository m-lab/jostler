// Package gcs handles downloading and uploading files to Gogole Cloud
// Storage (GCS).
//
// The clients in the following methods will use default application
// credentials ~/.config/gcloud/application_default_credentials.json.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
)

type StorageClient struct {
	bucket       string
	client       stiface.Client
	bucketHandle stiface.BucketHandle
}

var (
	downloadTimeout = 2 * time.Minute
	uploadTimeout   = time.Hour // same as pusher

	errCreateClient   = errors.New("failed to create GCS client")
	errDownloadObject = errors.New("failed to download GCS object")
	errUploadObject   = errors.New("failed to upload GCS object")
	errCloseObject    = errors.New("failed to close GCS object")

	// Testing and debugging support.
	storageNewClient = storage.NewClient
	verbose          = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// NewClient returns a new GCS client for the specified bucket.
// The return value is an interface to facilitate testing.
func NewClient(ctx context.Context, bucket string) (*StorageClient, error) {
	verbose("creating new storage client for %v", bucket)
	client, err := storageNewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCreateClient, err)
	}
	adaptClient := stiface.AdaptClient(client)
	return newStorageClient(bucket, stiface.AdaptClient(client), adaptClient.Bucket(bucket)), nil
}

func newStorageClient(bucket string, client stiface.Client, bucketHandle stiface.BucketHandle) *StorageClient {
	return &StorageClient{
		bucket:       bucket,
		client:       client,
		bucketHandle: bucketHandle,
	}
}

// Download downloads the specified object from GCS.
func (s *StorageClient) Download(ctx context.Context, objPath string) ([]byte, error) {
	verbose("downloading '%v:%v'", s.bucket, objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, downloadTimeout)
	defer storageCancel()
	obj := s.bucketHandle.Object(objPath)
	reader, err := obj.NewReader(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("'%v:%v': %w", s.bucket, objPath, err)
	}
	defer reader.Close()
	contents, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errDownloadObject, err)
	}
	verbose("'%v:%v' %v bytes", s.bucket, objPath, len(contents))
	return contents, nil
}

// Upload uploads the specified contents to GCS.
//
// Methods in the storage package may retry calls that fail with transient
// errors. Retrying continues indefinitely unless the controlling context is
// canceled, the client is closed, or a non-transient error is received.
func (s *StorageClient) Upload(ctx context.Context, objPath string, contents []byte) error {
	verbose("uploading '%v:%v'", s.bucket, objPath)
	obj := s.bucketHandle.Object(objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, uploadTimeout)
	defer storageCancel()
	writer := obj.NewWriter(storageCtx)
	for written := 0; written < len(contents); {
		n, err := fmt.Fprint(writer, string(contents[written:]))
		if err != nil {
			return fmt.Errorf("%w: %v", errUploadObject, err)
		}
		written += n
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("%w: %v", errCloseObject, err)
	}
	verbose("successfully uploaded '%v:%v' to GCS %v bytes", s.bucket, objPath, len(contents))
	return nil
}
