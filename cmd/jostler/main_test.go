// Package main implements jostler.
package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/jostler/api"
	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
)

const (
	testNode         = "mlab1-lga01.mlab-sandbox.measurement-lab.org"
	testNodeFile     = "@testdata/hostname"
	testLocalDataDir = "testdata"    // typically /var/spool
	testBucket       = "disk-bucket" // typically pusher-mlab-sandbox
	testExperiment   = "jostler"
	testDatatype     = "foo1"
)

var (
	errString  = errors.New("panic string")
	errError   = errors.New("panic error")
	errUnknown = errors.New("panic unknown")
)

// TestCLI tests non-interactive CLI invocations.
func TestCLI(t *testing.T) {
	// Prevent "bind: address already in use" errors during tests.
	addr := ":0"
	prometheusx.ListenAddress = &addr
	tests := []struct {
		name            string   // name of the test
		rmTblSchemaFile bool     // if true, remove table schema file before running the test
		wantErrStr      string   // error message
		args            []string // flags and arguments
	}{
		// Command line usage.
		{
			"local: help", false, flag.ErrHelp.Error(),
			[]string{"-h"},
		},
		// Invalid command lines.
		{
			"extra args", false, errExtraArgs.Error(),
			[]string{"extra-arg"},
		},
		{
			"undefined flag", false, "provided but not defined",
			[]string{"-undefined-flag"},
		},
		{
			"more datatype schemas than datatypes", false, errSchemaNums.Error(),
			[]string{
				"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype,
				"-datatype-schema-file", "foo1.json",
				"-datatype-schema-file", "foo2.json",
			},
		},
		// Invalid local mode command lines.
		{
			"local: non-existent default datatype schema file", false, schema.ErrReadSchema.Error(),
			[]string{"-local", "-experiment", testExperiment, "-datatype", testDatatype},
		},
		{
			"local: non-existent specified datatype schema file", false, schema.ErrReadSchema.Error(),
			[]string{"-local", "-experiment", testExperiment, "-datatype", testDatatype, "-datatype-schema-file", "foo1:testdata/datatypes/foo1-non-existent.json"},
		},
		{
			"local: invalid foo1", false, schema.ErrUnmarshal.Error(),
			[]string{"-local", "-experiment", testExperiment, "-datatype", "foo1", "-datatype-schema-file", "foo1:testdata/datatypes/foo1-invalid.json"},
		},
		// Valid local mode command lines.
		{
			"local: valid foo1", false, "",
			[]string{"-local", "-experiment", testExperiment, "-datatype", "foo1", "-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json"},
		},
		// Invalid daemon mode command lines.
		{
			"daemon: no node", false, errNoNode.Error(),
			[]string{"-gcs-bucket", testBucket, "-experiment", testExperiment, "-datatype", testDatatype},
		},
		{
			"daemon: no bucket", false, errNoBucket.Error(),
			[]string{"-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype},
		},
		{
			"daemon: no experiment", false, errNoExperiment.Error(),
			[]string{"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-datatype", testDatatype},
		},
		{
			"daemon: no datatype", false, errNoDatatype.Error(),
			[]string{"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment},
		},
		{
			"daemon: invalid hostname", false, "invalid hostname",
			[]string{"-gcs-bucket", testBucket, "-mlab-node-name", "hostname", "-experiment", testExperiment, "-datatype", testDatatype},
		},
		{
			"daemon: bad datatype schema filename", false, errSchemaFilename.Error(),
			[]string{
				"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype,
				"-datatype-schema-file", "foo1.json",
			},
		},
		{
			"daemon: mismatch between datatype and schema filename", false, errSchemaNoMatch.Error(),
			[]string{
				"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype,
				"-datatype-schema-file", "bar1:testdata/datatypes/foo1-valid.json",
			},
		},
		{
			"daemon: non-existent default datatype schema file", false, schema.ErrReadSchema.Error(),
			[]string{"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype},
		},
		{
			"daemon: non-existent specified datatype schema file", false, schema.ErrReadSchema.Error(),
			[]string{
				"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", testDatatype,
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-non-existent.json",
			},
		},
		{
			"daemon: invalid foo1", false, schema.ErrUnmarshal.Error(),
			[]string{"-gcs-bucket", testBucket, "-mlab-node-name", testNode, "-experiment", testExperiment, "-datatype", "foo1", "-datatype-schema-file", "foo1:testdata/datatypes/foo1-invalid.json"},
		},
		// Valid daemon mode command lines. The following four
		// tests cover the four scenarios described in main.go
		// (the order is important).
		// Scenario 1 - old doesn't exist, should upload.
		// Scenario 2 - old exists and matches new, should not upload.
		// Scenario 3 - new is a superset of old, should upload.
		// Scenario 4 - new is incompatible with old, should not upload.
		{
			"daemon: scenario 1", true, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v1",
			},
		},
		{
			"daemon: scenario 2", false, "",
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v1",
			},
		},
		{
			"daemon: scenario 3", false, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid-superset.json",
				"-gcs-data-dir=testdata/autoload/v1",
			},
		},
		{
			"daemon: scenario 4", false, schema.ErrOnlyInOld.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v1",
			},
		},
		// autoload/v2 flag and configuration testing.
		{
			"invalid: scenario 1", false, errAutoloadOrgInvalid.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v1",
				"-organization=bar1", // Should not specify organization for an autoload/v1 run.
			},
		},
		{
			"invalid: scenario 2", true, errAutoloadOrgRequired.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2", // any value other than autoload/v1.
				"-organization=",                     // Organization is required.
			},
		},
		{
			"invalid: scenario 3", true, errOrgName.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2", // any value other than autoload/v1.
				"-organization=INVALIDNAME",          // Organization is invalid.
			},
		},
		{
			"valid: scenario 4 - upload authoritative new schema", true, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=true", // allow uploads.
			},
		},
		{
			"valid: scenario 4 - allow matching schema without upload", false, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=false",
			},
		},
		{
			"invalid: scenario 4 - cannot upload new v2 schema", false, schema.ErrNewFields.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid-superset.json", // superset schema.
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=false",
			},
		},
		{
			"valid: scenario 5 - upload newer authoritative new schema", true, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid-superset.json",
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=true", // allow uploads.
			},
		},
		{
			"valid: scenario 5 - allow backward compatible schema", false, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=false",
			},
		},
		{
			"valid: scenario 5 - with hostname from file", false, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNodeFile,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v2",
				"-organization=foo1org",
				"-upload-schema=false",
			},
		},
		// autoload/v3 flag and configuration testing.
		{
			"invalid: v3 without organization", true, errAutoloadOrgRequired.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v3",
			},
		},
		{
			"invalid: v3 with hyphen in organization", true, errOrgName.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-local-data-dir", testLocalDataDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
				"-gcs-data-dir=testdata/autoload/v3",
				"-organization=foo-bar", // Organization is invalid.
			},
		},
	}
	defer func() {
		os.RemoveAll("foo1.json")
		os.RemoveAll("testdata/autoload")
	}()
	for i, test := range tests {
		t.Logf("name: %s", test.name)
		if test.rmTblSchemaFile {
			os.RemoveAll("testdata/autoload/v1/tables/jostler/foo1.table.json")
			os.RemoveAll("testdata/autoload/v2/tables/jostler/foo1.table.json")
		}
		var s string
		if test.wantErrStr == "" {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf("%s>>> test %02d: %s: %v%s", testhelper.ANSIPurple, i, s, test.name, testhelper.ANSIEnd)
		args := test.args
		// Use a local disk storage implementation that mimics downloads
		// from and uploads to GCS.
		args = append(args, []string{"-gcs-local-disk"}...)
		if testing.Verbose() {
			args = append(args, "-verbose")
		}
		callMain(t, args, test.wantErrStr)
	}
}

// TestGCSDirs tests the GCS data and index directories of each autoload
// version.
func TestGCSDirs(t *testing.T) {
	tests := []struct {
		name         string
		dataDir      string
		org          string
		wantDataDir  string
		wantIndexDir string
	}{
		{
			"v1", "autoload/v1", "",
			"autoload/v1/jostler/foo1", "autoload/v1/jostler/index1",
		},
		{
			"v2", "autoload/v2", "foo1org",
			"autoload/v2/foo1org/jostler/foo1", "autoload/v2/foo1org/jostler/index1",
		},
		{
			"v3", "autoload/v3", "foo1org",
			"autoload/v3/org=foo1org/jostler/foo1", "autoload/v3/org=foo1org/jostler/index1",
		},
		{
			"v3 with prefix", "testdata/autoload/v3", "foo1org",
			"testdata/autoload/v3/org=foo1org/jostler/foo1", "testdata/autoload/v3/org=foo1org/jostler/index1",
		},
	}
	for _, test := range tests {
		gotDataDir, gotIndexDir := gcsDirs(test.dataDir, test.org, testExperiment, testDatatype)
		if gotDataDir != test.wantDataDir || gotIndexDir != test.wantIndexDir {
			t.Fatalf("%s: gcsDirs() = %v, %v, wanted %v, %v", test.name, gotDataDir, gotIndexDir, test.wantDataDir, test.wantIndexDir)
		}
	}
}

// TestV3Upload runs jostler with autoload/v3 conventions and verifies
// that the data bundle and its index are uploaded to the org=<name>
// directory and that ArchiveURL of the bundled row has the same layout.
func TestV3Upload(t *testing.T) {
	addr := ":0"
	prometheusx.ListenAddress = &addr
	// The directory watcher notifies with absolute pathnames, so the
	// local data directory has to be absolute for files to be bundled.
	tmpDir := t.TempDir()
	const date = "2026/11/23"
	gcsDir := filepath.Join(tmpDir, "autoload/v3")
	dateDir := filepath.Join(tmpDir, "spool", testExperiment, testDatatype, date)
	if err := os.MkdirAll(dateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() = %v", err)
	}
	// Create a measurement file after jostler has started watching.
	go func() {
		time.Sleep(time.Second)
		os.WriteFile(filepath.Join(dateDir, "v3.json"), []byte(`{"Field1":3}`), 0o666)
	}()
	callMain(t, []string{
		"-gcs-bucket", "newclient,download,upload",
		"-mlab-node-name", testNode,
		"-local-data-dir", filepath.Join(tmpDir, "spool"),
		"-experiment", testExperiment,
		"-datatype", "foo1",
		"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
		"-gcs-data-dir", gcsDir,
		"-organization=foo1org",
		"-bundle-age-max", "1s",
		"-test-interval", "4s",
		"-gcs-local-disk",
	}, "")

	const wantDir = "autoload/v3/org=foo1org/jostler/foo1/" + date + "/"
	for _, dir := range []string{"index1", "foo1"} {
		objs, err := filepath.Glob(filepath.Join(gcsDir, "org=foo1org", testExperiment, dir, date, "*.jsonl.gz"))
		if err != nil || len(objs) != 1 {
			t.Fatalf("Glob(%v) = %v, %v, wanted one object", dir, objs, err)
		}
		if dir == "index1" {
			continue
		}
		f, err := os.Open(objs[0])
		if err != nil {
			t.Fatalf("Open() = %v", err)
		}
		defer f.Close()
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatalf("NewReader() = %v", err)
		}
		row, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("ReadAll() = %v", err)
		}
		var stdCols struct{ Archiver api.ArchiverV0 }
		if err := json.Unmarshal(row, &stdCols); err != nil {
			t.Fatalf("Unmarshal() = %v", err)
		}
		if !strings.Contains(stdCols.Archiver.ArchiveURL, wantDir) {
			t.Fatalf("ArchiveURL = %v, wanted it to contain %v", stdCols.Archiver.ArchiveURL, wantDir)
		}
	}
}

// callMain calls main() with the given command line in osArgs, expecting
// an error that will include the given string in wantErrStr (which could
// be the empty string "").
//
// Since flags are global variables, we need to create a new flag set before
// calling main().  Also, we need to change the behavior of fatal to panic
// instead of exiting in order to recover from fatal errors.
func callMain(t *testing.T, osArgs []string, wantErrStr string) {
	t.Helper()
	saveOSArgs := os.Args
	saveFatal := fatal
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.PanicOnError)
	defer func() {
		gotErr := recoverError(recover())
		if gotErr == nil {
			if wantErrStr != "" {
				t.Fatalf("main() = nil, wanted %v", wantErrStr)
			}
		} else {
			if wantErrStr == "" {
				t.Fatalf("main() = %v, wanted \"\"", gotErr)
			}
			gotErrStr := strings.ToLower(gotErr.Error())
			if !strings.Contains(gotErrStr, strings.ToLower(wantErrStr)) {
				t.Fatalf("main() = %v, wanted %v", gotErr, wantErrStr)
			}
		}
		os.Args = saveOSArgs
		fatal = saveFatal
	}()
	// Reset flags with global state.
	mlabNodeName = flagx.StringFile{}

	os.Args = []string{"jostler-test", "-test-interval", "2s"}
	os.Args = append(os.Args, osArgs...)
	fatal = log.Panic
	t.Logf("%s>>> %v%s", testhelper.ANSIPurple, strings.Join(os.Args, " "), testhelper.ANSIEnd)
	main()
}

// recoverError returns the error that caused the panic.
func recoverError(r any) error {
	if r == nil {
		return nil
	}
	var err error
	switch x := r.(type) {
	case string:
		err = fmt.Errorf("%w: %v", errString, x)
	case error:
		err = fmt.Errorf("%w: %v", errError, x)
	default:
		err = fmt.Errorf("%w: %v", errUnknown, x)
	}
	return err
}
