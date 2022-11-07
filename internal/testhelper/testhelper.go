// Package testhelper implements code that helps in unit and integration
// testing.  The helpers in this package include verbose logging (with
// colored details) and a fake cloud storage implementation that writes to
// the local filesystem instead of GCS.
package testhelper

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/schema"
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

// Fake downloading from and uploading to GCS.
//
// To provide strict testing, each test client should set the bucket name to
// the operation(s) it expects that particular test to perform.  An empty
// bucket name means no GCS operation is expected.  To force a failure,
// the operation name should be prefixed by "fail".
type fakeStorageClient struct {
	bucket string
}

// FakeNewClient creates and returns a fake storage client that will
// read from and write to the testdata directory on local filesystem.
func FakeNewClient(ctx context.Context, bucket string) (gcs.GCSClient, error) { //nolint:ireturn
	if !strings.Contains(bucket, "newclient") {
		panic("unexpected call to NewClient()")
	}
	if bucket == "failnewclient" {
		return nil, schema.ErrStorageClient
	}
	return &fakeStorageClient{bucket: bucket}, nil
}

// Download fakes downloading from GCS.
func (f *fakeStorageClient) Download(ctx context.Context, objPath string) ([]byte, error) {
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

// Upload fakes uploading to GCS.
func (f *fakeStorageClient) Upload(ctx context.Context, objPath string, contents []byte) error {
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
