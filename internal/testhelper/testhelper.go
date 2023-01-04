// Package testhelper implements code that helps in unit and integration
// testing.  The helpers in this package include verbose logging (with
// colored details) and a local disk storage implementation that mimics
// downloads from and uploads to cloud storage (GCS).
package testhelper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/watchdir"
)

const (
	ANSIGreen  = "\033[00;32m"
	ANSIBlue   = "\033[00;34m"
	ANSIPurple = "\033[00;35m"
	ANSIEnd    = "\033[0m"
)

// VLogf logs messages in verbose mode (mostly for debugging).  Messages
// are prefixed by "filename:line-number function()" printed in green and
// the message printed in blue for easier visual inspection.
func VLogf(format string, args ...interface{}) {
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Printf(format, args...)
		return
	}
	details := runtime.FuncForPC(pc)
	if details == nil {
		log.Printf(format, args...)
		return
	}
	file = filepath.Base(file)
	idx := strings.LastIndex(details.Name(), "/")
	if idx == -1 {
		idx = 0
	} else {
		idx++
	}
	a := []interface{}{ANSIGreen, file, line, details.Name()[idx:], ANSIBlue}
	a = append(a, args...)
	log.Printf("%s%s:%d: %s(): %s"+format+"%s", append(a, ANSIEnd)...)
}

// StorageClient implements a local disk storage that mimics downloads
// from and uploads to cloud storage (GCS) performed by the gcs package.
//
// To provide strict testing, each test client should set the bucket name to
// the operation(s) it expects that particular test to perform.  An empty
// bucket name means no GCS operation is expected.  To force a failure,
// the operation name should be prefixed by "fail".
type StorageClient struct {
	bucket string
}

// NewClient creates and returns a new client that mimics the upload
// method of a cloud storage client on the local disk.
func NewClient(ctx context.Context, bucket string) (*StorageClient, error) {
	if !strings.Contains(bucket, "newclient") {
		panic("unexpected call to NewClient()")
	}
	if bucket == "failnewclient" {
		return nil, schema.ErrStorageClient
	}
	return &StorageClient{
		bucket: bucket,
	}, nil
}

// Download mimics downloading from GCS.
func (d *StorageClient) Download(ctx context.Context, objPath string) ([]byte, error) {
	fmt.Printf("StorageClient.Download(): d.bucket=%v objPath=%v\n", d.bucket, objPath) //nolint:forbidigo
	if !strings.HasPrefix(objPath, "testdata") {
		objPath = filepath.Join("testdata", objPath)
	}
	if !strings.Contains(d.bucket, "download") {
		panic("unexpected call to Download()")
	}
	if strings.Contains(d.bucket, "faildownload") {
		return nil, schema.ErrDownload
	}
	contents, err := os.ReadFile(objPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrObjectNotExist
		}
		return nil, err //nolint:wrapcheck
	}
	return contents, nil
}

// Upload mimics uploading to GCS.
func (d *StorageClient) Upload(ctx context.Context, objPath string, contents []byte) error {
	fmt.Printf("StorageClient.upload(): d.bucket=%v objPath=%v len(contents)=%v\n", d.bucket, objPath, len(contents)) //nolint:forbidigo
	if !strings.HasPrefix(objPath, "testdata") {
		objPath = filepath.Join("testdata", objPath)
	}
	if !strings.Contains(d.bucket, "upload") {
		panic("unexpected call to Upload()")
	}
	if strings.Contains(d.bucket, "failupload") {
		return schema.ErrUpload
	}
	idx := strings.LastIndex(objPath, "/") // autoload/v0/tables/<experiment>/<datatype>.table.json
	if idx == -1 {
		panic("Upload(): objPath")
	}
	if err := os.MkdirAll(objPath[:idx], 0o755); err != nil {
		if !os.IsExist(err) {
			panic("Upload(): MkdirAll")
		}
	}
	return os.WriteFile(objPath, contents, 0o666) //nolint:wrapcheck
}

// WatchDir implements a directory watcher that mimics the watchdir
// package.
type WatchDir struct {
	watchDir     string
	watchChan    chan watchdir.WatchEvent
	watchAckChan chan []string
}

// WatchDirNew creates a new instance of WatchDir and returns it.
func WatchDirNew(watchDir string) (*WatchDir, error) {
	return &WatchDir{
		watchDir:     watchDir,
		watchChan:    make(chan watchdir.WatchEvent, 100),
		watchAckChan: make(chan []string, 100),
	}, nil
}

// WatchChan returns the channel through which watch events (paths)
// are sent to the client.
func (w *WatchDir) WatchChan() chan watchdir.WatchEvent {
	return w.watchChan
}

// WatchAckChan returns the channel through which client acknowledges
// the watch events it has received and processed, so watchdir can remove
// them from its notifiedFiles map.
func (w *WatchDir) WatchAckChan() chan<- []string {
	return w.watchAckChan
}

// WatchAndNotify watches a directory (and possibly all its subdirectories)
// for the configured events and sends the pathnames of the events it received
// through the configured channel.
func (w *WatchDir) WatchAndNotify(ctx context.Context) error {
	return nil
}
