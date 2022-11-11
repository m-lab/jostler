// Package schema_test implements black-box unit testing for package schema.
package schema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
)

const (
	testExperiment = "jostler"
	testDatatype   = "foo1"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	schema.Verbose(func(fmt string, args ...interface{}) {})
}

func TestPathForDatatype(t *testing.T) { //nolint:paralleltest
	// Since PathForDatatype() should not download from or upload
	// to GCS, set bucket to "" to force a panic in fake GCS.
	tests := []struct {
		bucket        string
		datatype      string
		dtSchemaFiles []string
		want          string
	}{
		{
			bucket:        "fake-bucket",
			datatype:      testDatatype,
			dtSchemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:          "/path/to/foo1.json",
		},
		{
			bucket:        "fake-bucket",
			datatype:      "baz1",
			dtSchemaFiles: []string{"foo1:/path/to/foo1.json", "bar1:/path/to/bar1.json"},
			want:          "/var/spool/datatypes/baz1.json",
		},
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d%s", testhelper.ANSIPurple, i, testhelper.ANSIEnd)
		dtSchemaFile := schema.PathForDatatype(test.datatype, test.dtSchemaFiles)
		if dtSchemaFile != test.want {
			t.Fatalf("PathForDatatype() = %v, want: %v", dtSchemaFile, test.want)
		}
	}
}

func TestValidateAndUpload(t *testing.T) { //nolint:paralleltest,funlen
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

		wantErr error
	}{
		{
			name:            "non-existent datatype schema file, should not upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "fake-bucket",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/non-existent.json", // this file doesn't exist
			wantErr:         schema.ErrReadSchema,
		},
		{
			name:            "invalid datatype schema file, should not upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "fake-bucket",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-invalid.json", // this file doesn't exist
			wantErr:         schema.ErrUnmarshal,
		},
		{
			name:            "force storage client creation",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "failnewclient",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         schema.ErrStorageClient,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload, but force upload failure",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: true,
			bucket:          "newclient,download,failupload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         schema.ErrUpload,
		},
		{
			name:            "scenario 1 - old doesn't exist, should upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: true,
			bucket:          "newclient,download,upload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         nil,
		},
		{
			name:            "scenario 2 - old exists, new matches, but force download failure",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,faildownload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         schema.ErrDownload,
		},
		{
			name:            "scenario 2 - old exists, new matches, should not upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         nil,
		},
		{
			name:            "scenario 3 - old exists, new is a superset, should upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download,upload",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid-superset.json",
			wantErr:         nil,
		},
		{
			name:            "scenario 4 - old exists, new is incompatible due to missing field mismatch, should not upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-valid.json",
			wantErr:         schema.ErrOnlyInOld,
		},
		{
			name:            "scenario 4 - old exists, new is incompatible due to field types, should not upload",
			tblSchemaFile:   "autoload/v0/tables/jostler/foo1.table.json",
			rmTblSchemaFile: false,
			bucket:          "newclient,download",
			experiment:      testExperiment,
			datatype:        testDatatype,
			dtSchemaFile:    "testdata/datatypes/foo1-incompatible.json",
			wantErr:         schema.ErrTypeMismatch,
		},
	}

	// Use a fake GCS implementation that reads from and writes to
	// the local filesystemi.
	saveGCSClient := schema.GCSClient
	schema.GCSClient = testhelper.FakeNewClient
	defer func() {
		schema.GCSClient = saveGCSClient
		os.RemoveAll("testdata/autoload")
	}()
	for i, test := range tests {
		if test.rmTblSchemaFile {
			os.RemoveAll(filepath.Join("testdata", test.tblSchemaFile))
		}
		var s string
		if test.wantErr == nil {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf("%s>>> test %02d: %s: %v%s", testhelper.ANSIPurple, i, s, test.name, testhelper.ANSIEnd)
		gotErr := schema.ValidateAndUpload(test.bucket, test.experiment, test.datatype, test.dtSchemaFile)
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