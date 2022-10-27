// Package main implements jostler.
package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	testBucket     = "pusher-mlab-sandbox"
	testNode       = "mlab1-lga01.mlab-sandbox.measurement-lab.org"
	testExpr       = "ndt"
	testDatatype   = "scamper1"
	testSchemaFile = "scamper1:testdata/scamper1-schema.json"
)

// TestCLI tests non-interactive CLI invocations.
func TestCLI(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		wantErrStr string
		args       []string
	}{
		// Interactive mode.
		{"help", flag.ErrHelp.Error(), []string{"-h"}},
		{"non-existent default schema file", errReadSchema.Error(), []string{
			"-local",
			"-experiment", testExpr,
			"-datatype", testDatatype,
		}},
		{"invalid foo1", errUnmarshal.Error(), []string{
			"-local",
			"-experiment", testExpr,
			"-datatype", "foo1",
			"-schema-file", "foo1:testdata/foo1-invalid-schema.json",
		}},
		{"valid foo1", "", []string{
			"-local",
			"-experiment", testExpr,
			"-datatype", "foo1",
			"-schema-file", "foo1:testdata/foo1-valid-schema.json",
		}},
		// Non-interactive mode.
		{"undefined flag", "provided but not defined", []string{"-undefined-flag"}},
		{"extra args", errExtraArgs.Error(), []string{"extra-arg"}},
		{"no node", errNoNode.Error(), []string{
			"-gcs-bucket", testBucket,
			"-experiment", testExpr,
			"-datatype", testDatatype,
		}},
		{"no bucket", errNoBucket.Error(), []string{
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
		}},
		{"no experiment", errNoExperiment.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-datatype", testDatatype,
		}},
		{"no datatype", errNoDatatype.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
		}},
		{"invalid hostname", "Invalid hostname", []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", "hostname",
			"-experiment", testExpr,
			"-datatype", testDatatype,
		}},
		{"unequal schemas and datatypes", errSchemaNums.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
			"-schema-file", "schema1.json",
			"-schema-file", "schema2.json",
		}},
		{"bad schema filename", errSchemaFilename.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
			"-schema-file", "schema.json",
		}},
		{"schema datatype mismatch", errSchemaNoMatch.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
			"-schema-file", "invalid:schema.json",
		}},
		{"non-existent specified schema file", errReadSchema.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
			"-schema-file", testSchemaFile,
		}},
		{"non-existent default schema file", errReadSchema.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", testDatatype,
		}},
		{"invalid foo1", errUnmarshal.Error(), []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", "foo1",
			"-schema-file", "foo1:testdata/foo1-invalid-schema.json",
		}},
		{"good invocation, upload not needed", "", []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", "foo1",
			"-schema-file", "foo1:testdata/foo1-valid-schema.json",
		}},
		{"good invocation, upload needed", "", []string{
			"-gcs-bucket", testBucket,
			"-mlab-node-name", testNode,
			"-experiment", testExpr,
			"-datatype", "foo1",
			"-schema-file", "foo1:testdata/foo1-valid-superset-schema.json",
		}},
	}
	for i, test := range tests {
		t.Logf("\n\n>>> running test %02d: %v", i, test.name)
		t.Logf(">>> %v", strings.Join(test.args, " "))
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
