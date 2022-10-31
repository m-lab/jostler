// Package main implements jostler.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/schema"
)

type (
	download func(context.Context, string, string) ([]byte, error) // gcs.Download signature
	upload   func(context.Context, string, string, []byte) error   // gcs.Upload signature
)

const (
	testNode       = "mlab1-lga01.mlab-sandbox.measurement-lab.org"
	testBucket     = "pusher-mlab-sandbox"
	testObject     = "autoload/v0/datatypes/jostler/foo1-schema.json"
	testExperiment = "jostler"
	testDatatype   = "foo1"
	testSchemaFile = "foo1:testdata/foo1:non-existent-schema.json"
)

// TestCLI tests non-interactive CLI invocations.
//
// The comment nolint:funlen,paralleltest tells golangci-lint
// not to run funlen and paralleltest linters because it's OK
// that the function length is more then 120 lines and also
// because we should not run these tests in parallel.
func TestCLI(t *testing.T) { //nolint:funlen,paralleltest
	saveGcsDownload := schema.GCSDownload
	saveGcsUpload := schema.GCSUpload
	defer func() {
		schema.GCSDownload = saveGcsDownload
		schema.GCSUpload = saveGcsUpload
	}()
	tests := []struct {
		name            string   // name of the test
		rmTblSchemaFile bool     // if true, remove table schema file before running the test
		wantErrStr      string   // error message
		args            []string // flags and arguments
		download        download // mock GCS download function
		upload          upload   // mock GCS upload function
	}{
		// Interactive mode.
		{
			"help", false, flag.ErrHelp.Error(),
			[]string{
				"-h",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"non-existent default schema file", false, errReadFile.Error(),
			[]string{
				"-local",
				"-experiment", testExperiment,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"invalid foo1", false, errUnmarshalFile.Error(),
			[]string{
				"-local",
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-invalid-schema.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"valid foo1", false, "",
			[]string{
				"-local",
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-valid-schema.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		// Non-interactive mode.
		{
			"undefined flag", false, "provided but not defined",
			[]string{
				"-undefined-flag",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"extra args", false, errExtraArgs.Error(),
			[]string{
				"extra-arg",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"no node", false, errNoNode.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"no bucket", false, errNoBucket.Error(),
			[]string{
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"no experiment", false, errNoExperiment.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"no datatype", false, errNoDatatype.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"invalid hostname", false, "Invalid hostname",
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", "hostname",
				"-experiment", testExperiment,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"unequal schemas and datatypes", false, errSchemaNums.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
				"-schema-file", "schema1.json",
				"-schema-file", "schema2.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"bad schema filename", false, errSchemaFilename.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
				"-schema-file", "schema.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"schema datatype mismatch", false, errSchemaNoMatch.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
				"-schema-file", "invalid:schema.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"non-existent specified schema file", false, errReadFile.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
				"-schema-file", testSchemaFile,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"non-existent default schema file", false, errReadFile.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", testDatatype,
			},
			panicGCSDownload, panicGCSUpload,
		},
		{
			"invalid foo1", false, errUnmarshalFile.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-invalid-schema.json",
			},
			panicGCSDownload, panicGCSUpload,
		},
		// The following four tests cover the four scenarios
		// described in main.go and the order is important.
		// Scenario 1 - old doesn't exist, should upload.
		// Scenario 2 - old exists and matches new, should not upload.
		// Scenario 3 - new is a superset of old, should upload.
		// Scenario 4 - new is incompatible with old, should not upload.
		{
			"scenario 1", true, "",
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-valid-schema.json",
			},
			fakeGCSDownload, fakeGCSUpload,
		},
		{
			"scenario 2", false, "",
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-valid-schema.json",
			},
			fakeGCSDownload, panicGCSUpload,
		},
		{
			"scenario 3", false, "",
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-valid-superset-schema.json",
			},
			fakeGCSDownload, fakeGCSUpload,
		},
		{
			"scenario 4", false, schema.ErrOnlyInOld.Error(),
			[]string{
				"-gcs-bucket", testBucket,
				"-mlab-node-name", testNode,
				"-experiment", testExperiment,
				"-datatype", "foo1",
				"-schema-file", "foo1:testdata/foo1-valid-schema.json",
			},
			fakeGCSDownload, panicGCSUpload,
		},
	}
	// Remove gs://pusher-mlab-sandbox/autoload/v0/datatypes/jostler/foo1-schema.json
	// before running the tests.
	if err := gcs.Delete(context.Background(), testBucket, testObject); err != nil {
		if !errors.Is(err, storage.ErrObjectNotExist) {
			t.Fatalf("'%v:%v': %v", testBucket, testObject, err)
		}
	}
	for i, test := range tests {
		if test.rmTblSchemaFile {
			os.RemoveAll("testdata/foo1-schema.json")
		}
		schema.GCSDownload = test.download
		schema.GCSUpload = test.upload
		var s string
		if test.wantErrStr == "" {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf(">>> test %02d: %s: %v", i, s, test.name)
		callMain(t, test.args, test.wantErrStr)
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
	os.Args = []string{"jostler-test", "-test-interval", "2s", "-verbose"}
	os.Args = append(os.Args, osArgs...)
	fatal = log.Panic
	t.Logf(">>> %v", strings.Join(os.Args, " "))
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

func panicGCSDownload(ctx context.Context, bucket, objPath string) ([]byte, error) {
	panic("unexpected call to gcs.Download()")
}

func panicGCSUpload(ctx context.Context, bucket, objPath string, tblSchemaJSON []byte) error {
	panic("unexpected call to gcs.Upload()")
}

func fakeGCSDownload(ctx context.Context, bucket, objPath string) ([]byte, error) {
	contents, err := os.ReadFile(filepath.Join("testdata", filepath.Base(objPath)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrObjectNotExist
		}
		return nil, err //nolint:wrapcheck
	}
	return contents, nil
}

func fakeGCSUpload(ctx context.Context, bucket, objPath string, tblSchemaJSON []byte) error {
	return os.WriteFile(filepath.Join("testdata", filepath.Base(objPath)), tblSchemaJSON, 0o666) //nolint:wrapcheck
}
