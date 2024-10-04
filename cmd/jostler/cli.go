// Package main implements jostler.
package main

import (
	"errors"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/host"
	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

var (
	// Flags related to GCS.
	bucket       string
	gcsDataDir   string
	mlabNodeName flagx.StringFile
	organization string
	uploadSchema bool = true

	// Flags related to bundles.
	dtSchemaFiles flagx.StringArray
	bundleSizeMax uint
	bundleAgeMax  time.Duration

	// Flags related to where to watch for data (inotify events).
	localDataDir   string
	extensions     flagx.StringArray
	experiment     string
	datatypes      flagx.StringArray
	missedAge      time.Duration
	missedInterval time.Duration

	// Flags related to program's execution.
	local        bool
	verbose      bool
	gcsLocalDisk bool
	testInterval time.Duration

	// Errors related to command line parsing and validation.
	errExtraArgs           = errors.New("extra arguments on the command line")
	errNoNode              = errors.New("must specify mlab-node-name or mlab-node-name-file")
	errNoBucket            = errors.New("must specify GCS bucket")
	errNoExperiment        = errors.New("must specify experiment")
	errNoDatatype          = errors.New("must specify at least one datatype")
	errSchemaNums          = errors.New("more schema files than datatypes")
	errSchemaNoMatch       = errors.New("does not match any specified datatypes")
	errSchemaFilename      = errors.New("is not in <datatype>:<pathname> format")
	errValidate            = errors.New("failed to validate")
	errAutoloadOrgRequired = errors.New("organization is required if not using autoload/v1 conventions")
	errAutoloadOrgInvalid  = errors.New("organization is not valid for autoload/v1 conventions")
	errOrgName             = errors.New("organization name must only contain lower case letters and numbers")

	// orgNameRegex matches valid organization names (a-z0-9, no spaces or capitals).
	orgNameRegex = regexp.MustCompile(`^[a-z0-9]+$`)
)

func initFlags() {
	// Flags related to GCS.
	flag.StringVar(&bucket, "gcs-bucket", "", "required - GCS bucket name")
	flag.StringVar(&gcsDataDir, "gcs-data-dir", "autoload/v1", "home directory in GCS bucket under which bundles will be uploaded")
	flag.Var(&mlabNodeName, "mlab-node-name", "required - node name, specified directly or via @file or via MLAB_NODE_NAME env variable")
	flag.StringVar(&organization, "organization", "", "the organization name; required for autoload/v2 conventions")
	flag.BoolVar(&uploadSchema, "upload-schema", true, "upload the local table schema if necessary")

	// Flags related to bundles.
	dtSchemaFiles = flagx.StringArray{}
	flag.UintVar(&bundleSizeMax, "bundle-size-max", 20*1024*1024, "maximum bundle size in bytes before it is uploaded")
	flag.DurationVar(&bundleAgeMax, "bundle-age-max", 1*time.Hour, "maximum bundle age before it is uploaded")

	// Flags related to where to watch for data (inotify events).
	flag.StringVar(&localDataDir, "local-data-dir", "/var/spool", "directory pathname under which measurement data is created")
	extensions = flagx.StringArray{".json"}
	flag.StringVar(&experiment, "experiment", "", "required - name of the experiment (e.g., ndt)")
	datatypes = flagx.StringArray{}
	flag.DurationVar(&missedAge, "missed-age", 3*time.Hour, "minimum duration since a file's last modification time before it is considered missed")
	flag.DurationVar(&missedInterval, "missed-interval", 30*time.Minute, "time interval between scans of filesystem for missed files")

	// Flags related to program's execution.
	flag.BoolVar(&local, "local", false, "run locally and create schema files for each datatype")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose mode")
	flag.BoolVar(&gcsLocalDisk, "gcs-local-disk", false, "use local disk storage instead of cloud storage (for test purposes only)")
	flag.DurationVar(&testInterval, "test-interval", 0, "time interval to stop running (for test purposes only)")

	flag.Var(&dtSchemaFiles, "datatype-schema-file", "schema for each datatype in the format <datatype>:<pathname>")
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
		gcs.Verbose(testhelper.VLogf)
		schema.Verbose(testhelper.VLogf)
		watchdir.Verbose(testhelper.VLogf)
		uploadbundle.Verbose(testhelper.VLogf)
	}

	if extensions == nil {
		extensions = []string{".json"}
	}
	if !local {
		if mlabNodeName.Value == "" {
			return errNoNode
		}
		if bucket == "" {
			return errNoBucket
		}
	}
	if experiment == "" {
		return errNoExperiment
	}
	if mlabNodeName.Value != "" {
		// Parse the M-Lab hostname (which should be in one of the
		// following formats) into its constituent parts.
		// v1: <machine>.<site>.measurement-lab.org
		// v2: <machine>-<site>.<project>.measurement-lab.org
		if _, err := host.Parse(mlabNodeName.Value); err != nil {
			return fmt.Errorf("failed to parse hostname: %w", err)
		}
	}
	if len(datatypes) == 0 {
		return errNoDatatype
	}
	if err := validateSchemaFlags(); err != nil {
		return err
	}
	if !strings.Contains(gcsDataDir, "autoload/v1") && organization == "" {
		return errAutoloadOrgRequired
	}
	if strings.Contains(gcsDataDir, "autoload/v1") && organization != "" {
		return errAutoloadOrgInvalid
	}
	if organization != "" && !orgNameRegex.MatchString(organization) {
		return errOrgName
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
// via the -datatype-schema-file flag.
func validateSchemaFiles() error {
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, dtSchemaFiles)
		if err := schema.ValidateSchemaFile(dtSchemaFile); err != nil {
			return fmt.Errorf("%v: %w", errValidate, err)
		}
	}
	return nil
}
