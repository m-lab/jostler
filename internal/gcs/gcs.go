// Package gcs handles downloading and uploading files to Gogole Cloud
// Storage (GCS).
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
)

var (
	uploadTimeout   = 5 * time.Second
	downloadTimeout = 5 * time.Second

	errReadGCS = errors.New("failed to read from GCS")

	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// Download downloads the specified file from GCS.
func Download(ctx context.Context, bucket, objPath string) ([]byte, error) {
	verbose("downloading '%v:%v'", bucket, objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, downloadTimeout)
	defer storageCancel()
	// Client will use default application credentials.
	// ~/.config/gcloud/application_default_credentials.json
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	bkt := client.Bucket(bucket)
	obj := bkt.Object(objPath)
	reader, err := obj.NewReader(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("'%v:%v': %w", bucket, objPath, err)
	}
	defer reader.Close()
	contents, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errReadGCS, err)
	}
	verbose("'%v:%v' %v bytes", bucket, objPath, len(contents))
	return contents, nil
}

// Upload uploads the specified contents to GCS.
//
// Methods in the storage package may retry calls that fail with transient
// errors. Retrying continues indefinitely unless the controlling context is
// canceled, the client is closed, or a non-transient error is received.
func Upload(ctx context.Context, bucket, objPath string, contents []byte) error {
	storageCtx, storageCancel := context.WithTimeout(ctx, uploadTimeout)
	defer storageCancel()
	// Client will use default application credentials.
	// ~/.config/gcloud/application_default_credentials.json
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	bkt := client.Bucket(bucket)
	obj := bkt.Object(objPath)
	writer := obj.NewWriter(storageCtx)
	for written := 0; written < len(contents); {
		n, err := fmt.Fprint(writer, string(contents[written:]))
		if err != nil {
			return fmt.Errorf("failed to write to GCS object: %w", err)
		}
		written += n
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS object: %w", err)
	}
	verbose("successfully uploaded '%v:%v' to GCS %v bytes", bucket, objPath, len(contents))
	return nil
}
