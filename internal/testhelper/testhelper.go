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
	"github.com/m-lab/jostler/internal/gcs"
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

// diskStorageClient implements a local disk storage that mimics downloads
// from and uploads to GCS.
//
// To provide strict testing, each test client should set the bucket name to
// the operation(s) it expects that particular test to perform.  An empty
// bucket name means no GCS operation is expected.  To force a failure,
// the operation name should be prefixed by "fail".
type diskStorageClient struct {
	bucket string
}

// DiskNewClient creates and returns a disk storage client that will
// read from and write to the testdata directory on the local filesystem.
func DiskNewClient(ctx context.Context, bucket string) (gcs.GCSClient, error) { //nolint:ireturn
	if !strings.Contains(bucket, "newclient") {
		panic("unexpected call to NewClient()")
	}
	if bucket == "failnewclient" {
		return nil, schema.ErrStorageClient
	}
	return &diskStorageClient{bucket: bucket}, nil
}

// Download mimics downloading from GCS.
func (f *diskStorageClient) Download(ctx context.Context, objPath string) ([]byte, error) {
	fmt.Printf("downloading from disk-bucket:%v\n", objPath) //nolint:forbidigo
	if !strings.Contains(f.bucket, "download") {
		panic("unexpected call to Download()")
	}
	if strings.Contains(f.bucket, "faildownload") {
		return nil, schema.ErrDownload
	}
	file := filepath.Join("testdata", objPath)
	contents, err := os.ReadFile(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrObjectNotExist
		}
		return nil, err //nolint:wrapcheck
	}
	return contents, nil
}

// Upload mimics uploading to GCS.
func (f *diskStorageClient) Upload(ctx context.Context, objPath string, contents []byte) error {
	fmt.Printf("uploading %d bytes to disk-bucket:%s\n", len(contents), objPath) //nolint:forbidigo
	if !strings.Contains(f.bucket, "upload") {
		panic("unexpected call to Upload()")
	}
	if strings.Contains(f.bucket, "failupload") {
		return schema.ErrUpload
	}
	idx := strings.LastIndex(objPath, "/") // autoload/v0/tables/<experiment>/<datatype>.table.json
	if idx == -1 {
		panic("Upload(): objPath")
	}
	dirs := filepath.Join("testdata", objPath[:idx])
	if err := os.MkdirAll(dirs, 0o755); err != nil {
		if !os.IsExist(err) {
			panic("Upload(): MkdirAll")
		}
	}
	file := filepath.Join("testdata", objPath)
	return os.WriteFile(file, contents, 0o666) //nolint:wrapcheck
}

type watchDirClient struct {
	watchDir     string
	watchChan    chan watchdir.WatchEvent
	watchAckChan chan []string
}

func WatchDirNew(watchDir string) (watchdir.WatchDirClient, error) { //nolint:ireturn
	return &watchDirClient{
		watchDir:     watchDir,
		watchChan:    make(chan watchdir.WatchEvent, 100),
		watchAckChan: make(chan []string, 100),
	}, nil
}

func (w *watchDirClient) WatchChan() chan watchdir.WatchEvent {
	return w.watchChan
}

func (w *watchDirClient) WatchAckChan() chan<- []string {
	return w.watchAckChan
}

func (w *watchDirClient) WatchAndNotify(ctx context.Context) error {
	return nil
}
