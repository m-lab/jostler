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
)

var (
	uploadTimeout   = time.Hour // same as pusher
	downloadTimeout = 2 * time.Minute
	deleteTimeout   = time.Minute

	errReadObject   = errors.New("failed to read GCS object")
	errWriteObject  = errors.New("failed to write GCS object")
	errDeleteObject = errors.New("failed to delete GCS object")

	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// Download downloads the specified object from GCS.
func Download(ctx context.Context, bucket, objPath string) ([]byte, error) {
	verbose("downloading '%v:%v'", bucket, objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, downloadTimeout)
	defer storageCancel()
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()
	obj := client.Bucket(bucket).Object(objPath)
	reader, err := obj.NewReader(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("'%v:%v': %w", bucket, objPath, err)
	}
	defer reader.Close()
	contents, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errReadObject, err)
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
	verbose("uploading '%v:%v'", bucket, objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, uploadTimeout)
	defer storageCancel()
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()
	obj := client.Bucket(bucket).Object(objPath)
	writer := obj.NewWriter(storageCtx)
	for written := 0; written < len(contents); {
		n, err := fmt.Fprint(writer, string(contents[written:]))
		if err != nil {
			return fmt.Errorf("%v: %w", errWriteObject, err)
		}
		written += n
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS object: %w", err)
	}
	verbose("successfully uploaded '%v:%v' to GCS %v bytes", bucket, objPath, len(contents))
	return nil
}

// Delete deletes the specified object from the specified bucket.
func Delete(ctx context.Context, bucket, objPath string) error {
	verbose("deleting '%v:%v'", bucket, objPath)
	storageCtx, storageCancel := context.WithTimeout(ctx, deleteTimeout)
	defer storageCancel()
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	obj := client.Bucket(bucket).Object(objPath)
	if err := obj.Delete(storageCtx); err != nil {
		return fmt.Errorf("%v: %w", errDeleteObject, err)
	}
	verbose("successfully deleted '%v:%v'", bucket, objPath)
	return nil
}
