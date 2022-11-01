// Package schema implements code that handles datatype and table schemas.
package schema //nolint:testpackage //nolint:testpackage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
)

const (
	testBucket = "fake-bucket"
	testObject = "autoload/v0/datatypes/jostler/foo1-schema.json"

	ansiGreen  = "\033[00;32m"
	ansiBlue   = "\033[00;34m"
	ansiPurple = "\033[00;35m"
	ansiEnd    = "\033[0m"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(fakeVerbosef)
}

func TestPathForDatatype(t *testing.T) { //nolint:paralleltest
	// PathForDatatype() should not download from or upload to GCS.
	saveGCSDownload := GCSDownload
	saveGCSUpload := GCSUpload
	defer func() {
		GCSDownload = saveGCSDownload
		GCSUpload = saveGCSUpload
	}()
	GCSDownload = panicGcsDownload
	GCSUpload = panicGcsUpload

	tests := []struct {
		datatype    string
		schemaFiles []string
		want        string
	}{
		{
			datatype:    "foo1",
			schemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:        "/path/to/foo1.json",
		},
		{
			datatype:    "baz1",
			schemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:        "/var/spool/datatypes/baz1/schema.json",
		},
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d%s", ansiPurple, i, ansiEnd)
		dtSchemaFile := PathForDatatype(test.datatype, test.schemaFiles)
		if dtSchemaFile != test.want {
			t.Fatalf("PathForDatatype() = %v, want: %v", dtSchemaFile, test.want)
		}
	}
}

func TestValidateAndUpload(t *testing.T) { //nolint:paralleltest,funlen
	saveGCSDownload := GCSDownload
	saveGCSUpload := GCSUpload
	defer func() {
		GCSDownload = saveGCSDownload
		GCSUpload = saveGCSUpload
	}()
	tests := []struct {
		name            string
		rmTblSchemaFile bool
		bucket          string
		object          string
		experiment      string
		datatype        string
		dtSchemaFile    string
		download        func(context.Context, string, string) ([]byte, error)
		upload          func(context.Context, string, string, []byte) error
		wantErr         error
	}{
		{
			name:            "non-existent datatype schema file, should not upload",
			rmTblSchemaFile: false,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1:non-existent-schema.json",
			download:        fakeGcsDownload,
			upload:          panicGcsUpload,
			wantErr:         ErrReadSchema,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload",
			rmTblSchemaFile: true,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1-valid-schema.json",
			download:        fakeGcsDownload,
			upload:          fakeGcsUpload,
			wantErr:         nil,
		},
		{
			name:            "scenario 2 - old exists and matches new, should not upload",
			rmTblSchemaFile: false,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1-valid-schema.json",
			download:        fakeGcsDownload,
			upload:          panicGcsUpload,
			wantErr:         nil,
		},
		{
			name:            "scenario 3 - new is a superset of old, should upload",
			rmTblSchemaFile: false,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1-valid-superset-schema.json",
			download:        fakeGcsDownload,
			upload:          fakeGcsUpload,
			wantErr:         nil,
		},
		{
			name:            "scenario 4 - new is incompatible with old due to missing field mismatch, should not upload",
			rmTblSchemaFile: false,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1-valid-schema.json",
			download:        fakeGcsDownload,
			upload:          panicGcsUpload,
			wantErr:         ErrOnlyInOld,
		},
		{
			name:            "scenario 4 - new is incompatible with old due to field types, should not upload",
			rmTblSchemaFile: false,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "foo1",
			dtSchemaFile:    "testdata/foo1-incompatible-schema.json",
			download:        fakeGcsDownload,
			upload:          panicGcsUpload,
			wantErr:         ErrTypeMismatch,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload, force upload failure",
			rmTblSchemaFile: true,
			bucket:          testBucket,
			object:          testObject,
			experiment:      "jostler",
			datatype:        "bar1",
			dtSchemaFile:    "testdata/foo1-valid-schema.json",
			download:        fakeGcsDownload,
			upload:          fakeGcsUpload,
			wantErr:         ErrUpload,
		},
	}

	for i, test := range tests {
		if test.rmTblSchemaFile {
			os.RemoveAll(fmt.Sprintf("testdata/%s-schema.json", test.datatype))
		}
		GCSDownload = test.download
		GCSUpload = test.upload
		var s string
		if test.wantErr == nil {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf("%s>>> test %02d: %s: %v%s", ansiPurple, i, s, test.name, ansiEnd)
		gotErr := ValidateAndUpload(test.bucket, test.experiment, test.datatype, test.dtSchemaFile)
		t.Logf("%s>>> gotErr=%v%s", ansiPurple, gotErr, ansiEnd)
		if gotErr == nil && test.wantErr == nil {
			continue
		}
		if (gotErr != nil && test.wantErr == nil) ||
			(gotErr == nil && test.wantErr != nil) ||
			!strings.Contains(gotErr.Error(), test.wantErr.Error()) {
			t.Fatalf("ValidateAndUpload() = %v, wanted %v", gotErr, test.wantErr)
		}
	}
}

func fakeVerbosef(format string, args ...interface{}) {
	if format == "" {
		return
	}
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		return
	}
	details := runtime.FuncForPC(pc)
	if details == nil {
		return
	}
	file = filepath.Base(file)
	idx := strings.LastIndex(details.Name(), "/")
	if idx == -1 {
		idx = 0
	} else {
		idx++
	}
	a := []interface{}{file, line, details.Name()[idx:]}
	a = append(a, args...)
	log.Printf("%s:%v: %s(): "+format+"\n", a...)
}

func panicGcsDownload(ctx context.Context, bucket, objPath string) ([]byte, error) {
	panic("unexpected call to gcs.Download()")
}

func panicGcsUpload(ctx context.Context, bucket, objPath string, tblSchemaJSON []byte) error {
	panic("unexpected call to gcs.Upload()")
}

func fakeGcsDownload(ctx context.Context, bucket, objPath string) ([]byte, error) {
	contents, err := os.ReadFile(filepath.Join("testdata", filepath.Base(objPath)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrObjectNotExist
		}
		return nil, err //nolint:wrapcheck
	}
	return contents, nil
}

func fakeGcsUpload(ctx context.Context, bucket, objPath string, tblSchemaJSON []byte) error {
	if strings.Contains(objPath, "bar1") {
		return ErrUpload
	}
	return os.WriteFile(filepath.Join("testdata", filepath.Base(objPath)), tblSchemaJSON, 0o666) //nolint:wrapcheck
}
