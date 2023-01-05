// Package main implements jostler.
package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
)

const (
	testNode        = "mlab1-lga01.mlab-sandbox.measurement-lab.org"
	testDataHomeDir = "testdata"    // typically /var/spool
	testBucket      = "disk-bucket" // typically pusher-mlab-sandbox
	testExperiment  = "jostler"
	testDatatype    = "foo1"
)

// TestCLI tests non-interactive CLI invocations.
//
// The comment nolint:funlen,paralleltest tells golangci-lint
// not to run funlen and paralleltest linters because it's OK
// that the function length is more then 120 lines and also
// because we should not run these tests in parallel.
func TestCLI(t *testing.T) { //nolint:funlen,paralleltest
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
			"daemon: invalid hostname", false, "Invalid hostname",
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
				"-data-home-dir", testDataHomeDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
			},
		},
		{
			"daemon: scenario 2", false, "",
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-data-home-dir", testDataHomeDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
			},
		},
		{
			"daemon: scenario 3", false, "",
			[]string{
				"-gcs-bucket", "newclient,download,upload",
				"-mlab-node-name", testNode,
				"-data-home-dir", testDataHomeDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid-superset.json",
			},
		},
		{
			"daemon: scenario 4", false, schema.ErrOnlyInOld.Error(),
			[]string{
				"-gcs-bucket", "newclient,download",
				"-mlab-node-name", testNode,
				"-data-home-dir", testDataHomeDir,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-datatype-schema-file", "foo1:testdata/datatypes/foo1-valid.json",
			},
		},
	}
	defer func() {
		os.RemoveAll("foo1.json")
		os.RemoveAll("testdata/autoload")
	}()
	for i, test := range tests {
		if test.rmTblSchemaFile {
			os.RemoveAll("testdata/autoload/v1/tables/jostler/foo1.table.json")
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
		args = append(args, "-local-disk")
		if testing.Verbose() {
			args = append(args, "-verbose")
		}
		callMain(t, args, test.wantErrStr)
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
			} else if !strings.Contains(gotErr.Error(), wantErrStr) {
				t.Fatalf("main() = %v, wanted %v", gotErr, wantErrStr)
			}
		}
		os.Args = saveOSArgs
		fatal = saveFatal
	}()
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
		err = errors.New(x) //nolint
	case error:
		err = x
	default:
		err = errors.New("unknown panic") //nolint
	}
	return err
}
