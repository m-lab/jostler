// Package main implements jostler.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/host"
	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

var (
	// Flags related to GCS.
	bucket       string
	gcsHomeDir   string
	mlabNodeName string

	// Flags related to bundles.
	dtSchemaFiles flagx.StringArray
	bundleSizeMax uint
	bundleAgeMax  time.Duration
	bundleNoRm    bool

	// Flags related to where to watch for data (inotify events).
	dataHomeDir    string
	extensions     flagx.StringArray
	experiment     string
	datatypes      flagx.StringArray
	missedAge      time.Duration
	missedInterval time.Duration

	// Flags related to program's execution.
	local        bool
	verbose      bool
	testInterval time.Duration

	// Errors related to command line parsing and validation.
	errExtraArgs      = errors.New("extra arguments on the command line")
	errNoNode         = errors.New("must specify mlab-node-name")
	errNoBucket       = errors.New("must specify GCS bucket")
	errNoExperiment   = errors.New("must specify experiment")
	errNoDatatype     = errors.New("must specify at least one datatype")
	errSchemaNums     = errors.New("more schema files than datatypes")
	errSchemaNoMatch  = errors.New("does not match any specified datatypes")
	errSchemaFilename = errors.New("is not in <datatype>:<pathname> format")
	errReadFile       = errors.New("failed to read file")
	errUnmarshalFile  = errors.New("failed to unmarshal file")
)

func initFlags() {
	// Flags related to GCS.
	flag.StringVar(&bucket, "gcs-bucket", "", "required - GCS bucket name")
	flag.StringVar(&gcsHomeDir, "gcs-home-dir", "autoload/v0", "home directory in GCS bucket under which bundles will be uploaded")
	flag.StringVar(&mlabNodeName, "mlab-node-name", "", "required - node name specified directly or via MLAB_NODE_NAME env variable")

	// Flags related to bundles.
	dtSchemaFiles = flagx.StringArray{}
	flag.UintVar(&bundleSizeMax, "bundle-size-max", 20*1024*1024, "maximum bundle size in bytes before it is uploaded")
	flag.DurationVar(&bundleAgeMax, "bundle-age-max", 1*time.Hour, "maximum bundle age before it is uploaded")
	flag.BoolVar(&bundleNoRm, "no-rm", false, "do not remove files of a bundle after successful upload") // XXX debugging support - delete when done

	// Flags related to where to watch for data (inotify events).
	flag.StringVar(&dataHomeDir, "data-home-dir", "/var/spool", "directory pathname under which experiment data is created")
	extensions = flagx.StringArray{".json"}
	flag.StringVar(&experiment, "experiment", "", "required - name of the experiment (e.g., ndt)")
	datatypes = flagx.StringArray{}
	flag.DurationVar(&missedAge, "missed-age", 3*time.Hour, "minimum duration since a file's last modification time before it is considered missed")
	flag.DurationVar(&missedInterval, "missed-interval", 30*time.Minute, "time interval between scans of filesystem for missed files")

	// Flags related to program's execution.
	flag.BoolVar(&local, "local", false, "run locally and create schema files for each datatype")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose mode")
	flag.DurationVar(&testInterval, "test-interval", 0, "time interval to stop running (for test purposes only)")

	flag.Var(&dtSchemaFiles, "dt-schema-file", "schema for each datatype in the format <datatype>:<pathname>")
	flag.Var(&extensions, "extensions", "filename extensions to watch within <data-dir>/<experiment>")
	flag.Var(&datatypes, "datatype", "required - datatype(s) to watch within <data-dir>/<experiment>")
}

// parseAndValidateCLI parses and validates the command line.
func parseAndValidateCLI() error {
	initFlags()
	// Note that extensions was declared as flags.StringArray{".json"}
	// so the usage message would show the right default value.
	// But we have to set it to nil before parsing the flags because
	// flagx.StringArray always appends to the array and there is no
	// way to remove an element from it.
	extensions = nil
	flag.Parse()
	if flag.NArg() != 0 {
		return errExtraArgs
	}

	// Now, check if some flags were set in the environment instead
	// of on the command line.
	if err := flagx.ArgsFromEnv(flag.CommandLine); err != nil {
		return fmt.Errorf("failed to get args from the environment: %w", err)
	}

	// Enable verbose mode in all packages as soon as the flags are
	// parsed because they may be called for during argument validation.
	if verbose {
		gcs.Verbose(vLogf)
		schema.Verbose(vLogf)
		watchdir.Verbose(vLogf)
		uploadbundle.Verbose(vLogf)
	}

	if extensions == nil {
		extensions = []string{".json"}
	}
	if !local {
		if mlabNodeName == "" {
			return errNoNode
		}
		if bucket == "" {
			return errNoBucket
		}
	}
	if experiment == "" {
		return errNoExperiment
	}
	if mlabNodeName != "" {
		// Parse the M-Lab hostname (which should be in one of the
		// following formats) into its constituent parts.
		// v1: <machine>.<site>.measurement-lab.org
		// v2: <machine>-<site>.<project>.measurement-lab.org
		if _, err := host.Parse(mlabNodeName); err != nil {
			return fmt.Errorf("failed to parse hostname: %w", err)
		}
	}
	if len(datatypes) == 0 {
		return errNoDatatype
	}
	if err := validateSchemaFlags(); err != nil {
		return err
	}
	return validateSchemaFiles()
}

// validateSchemaFlags validate that for each schema file, its corresponding
// datatype has been specified.
func validateSchemaFlags() error {
	if len(dtSchemaFiles) > len(datatypes) {
		return errSchemaNums
	}
	for _, schemaFile := range dtSchemaFiles {
		idx := strings.Index(schemaFile, ":")
		if idx == -1 {
			return fmt.Errorf("%v: %w", schemaFile, errSchemaFilename)
		}
		found := false
		for _, datatype := range datatypes {
			if datatype == schemaFile[:idx] {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%v: %w", schemaFile, errSchemaNoMatch)
		}
	}
	return nil
}

// validateSchemaFiles validates all datatype schema files exist and
// are well-formed JSON.
//
// The schema package is configured with the default path of datatype
// schema files but datatype schema files can also be explicitly specified
// via the -dt-schema-file flag.
func validateSchemaFiles() error {
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, dtSchemaFiles)
		// Does it exist?
		contents, err := os.ReadFile(dtSchemaFile)
		if err != nil {
			return fmt.Errorf("%v: %w", errReadFile, err)
		}
		// Is it well-formed JSON?
		var data []interface{}
		if err = json.Unmarshal(contents, &data); err != nil {
			return fmt.Errorf("%v: %v: %w", dtSchemaFile, errUnmarshalFile, err)
		}
	}
	return nil
}
