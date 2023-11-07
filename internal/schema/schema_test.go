// Package schema_test implements black-box unit testing for package schema.
package schema_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
)

const (
	testExperiment = "jostler"
	testDatatype   = "foo1"
)

func TestVerbose(t *testing.T) {
	schema.Verbose(func(fmt string, args ...interface{}) {})
}

func TestPathForDatatype(t *testing.T) {
	// Since PathForDatatype() should not download from or upload to
	// GCS, set bucket to "" to force a panic in the local disk storage
	// implementation.
	tests := []struct {
		bucket        string
		localDataDir  string
		datatype      string
		dtSchemaFiles []string
		want          string
	}{
		{
			bucket:        "",
			localDataDir:  "",
			datatype:      testDatatype,
			dtSchemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:          "/path/to/foo1.json",
		},
		{
			bucket:        "",
			localDataDir:  "",
			datatype:      "baz1",
			dtSchemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:          "/var/spool/datatypes/baz1.json",
		},
		{
			bucket:        "",
			localDataDir:  "/abc",
			datatype:      "baz1",
			dtSchemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:          "/abc/datatypes/baz1.json",
		},
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d%s", testhelper.ANSIPurple, i, testhelper.ANSIEnd)
		var saveLocalDataDir string
		if test.localDataDir != "" {
			saveLocalDataDir = schema.LocalDataDir
			schema.LocalDataDir = test.localDataDir
		}
		dtSchemaFile := schema.PathForDatatype(test.datatype, test.dtSchemaFiles)
		if dtSchemaFile != test.want {
			t.Fatalf("PathForDatatype() = %v, want: %v", dtSchemaFile, test.want)
		}
		if test.localDataDir != "" {
			schema.LocalDataDir = saveLocalDataDir
		}
	}
}

func TestValidateAndUpload(t *testing.T) {
	if testing.Verbose() {
		schema.Verbose(testhelper.VLogf)
		defer schema.Verbose(func(fmt string, args ...interface{}) {})
	}
	tests := []struct {
		name            string
		tblSchemaFile   string
		rmTblSchemaFile bool
		bucket          string
		experiment      string
		datatype        string
		dtSchemaFile    string
		uploadSchema    bool

		wantErr error
	}{
		{
			name:            "non-existent datatype schema file, should not upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/non-existent.json", // this file doesn't exist
			uploadSchema:    true,
			wantErr:         schema.ErrReadSchema,
		},
		{
			name:            "invalid datatype schema file, should not upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-invalid.json", // this file doesn't exist
			uploadSchema:    true,
			wantErr:         schema.ErrUnmarshal,
		},
		{
			name:            "force storage client creation failure",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "failnewclient",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         schema.ErrStorageClient,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload, but force upload failure",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: true,
			bucket:          "newclient,download,failupload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         schema.ErrUpload,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: true,
			bucket:          "newclient,download,upload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         nil,
		},
		{
			name:            "scenario 2 - old exists, new matches, but force download failure",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,faildownload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         schema.ErrDownload,
		},
		{
			name:            "scenario 2 - old exists, new matches, should not upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         nil,
		},
		{
			name:            "scenario 3 - old exists, new is a superset, should upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download,upload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid-superset.json",
			uploadSchema:    true,
			wantErr:         nil,
		},
		{
			name:            "scenario 4 - old exists, new is incompatible due to missing field mismatch, should not upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    true,
			wantErr:         schema.ErrOnlyInOld,
		},
		{
			name:            "scenario 4 - old exists, new is incompatible due to field types, should not upload",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-incompatible.json",
			uploadSchema:    true,
			wantErr:         schema.ErrTypeMismatch,
		},
		{
			name:            "scenario 5 no-upload - old exists, new is backward-compatible - should succeed",
			tblSchemaFile:   "autoload/v1/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			uploadSchema:    false,
			wantErr:         nil,
		},
	}

	defer func() {
		os.RemoveAll("testdata/autoload")
	}()
	for i, test := range tests {
		if test.rmTblSchemaFile {
			os.RemoveAll(test.tblSchemaFile)
		}
		var s string
		if test.wantErr == nil {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf("%s>>> test %02d: %s: %v%s", testhelper.ANSIPurple, i, s, test.name, testhelper.ANSIEnd)
		// Use a local disk storage implementation that mimics downloads
		// from and uploads to GCS.
		stClient, err := testhelper.NewClient(context.Background(), test.bucket)
		if err != nil {
			if errors.Is(err, test.wantErr) {
				continue // we expected this error
			}
			t.Fatalf("testhelper.NewClient() = %v, wanted nil", err)
		}
		gotErr := schema.ValidateAndUpload(stClient, test.bucket, test.experiment, test.datatype, test.dtSchemaFile, test.uploadSchema)
		t.Logf("%s>>> gotErr=%v%s\n\n", testhelper.ANSIPurple, gotErr, testhelper.ANSIEnd)
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
