// Package main implements jostler.
//
// We use log.Panicf() instead of log.Fatalf() because log.Fatalf()
// calls os.Exit() which will not run deferred calls and also makes
// testing harder (for testing, we can recover from log.Panicf()).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/host"
	"github.com/rjeczalik/notify"

	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

var (
	// Flags related to GCS.
	bucket       = flag.String("gcs-bucket", "", "GCS bucket name")
	gcsHomeDir   = flag.String("gcs-home-dir", "autoload/v0", "home directory in GCS bucket under which bundles will be uploaded")
	mlabNodeName = flag.String("mlab-node-name", "", "node name specified directly or via MLAB_NODE_NAME env variable")

	// Flags related to bundles.
	schemaFiles   = flagx.StringArray{}
	bundleSizeMax = flag.Uint("bundle-size-max", 20*1024*1024, "maximum bundle size in bytes before it is uploaded")
	bundleAgeMax  = flag.Duration("bundle-age-max", 1*time.Hour, "maximum bundle age before it is uploaded")
	bundleNoRm    = flag.Bool("no-rm", false, "do not remove files of a bundle after successful upload") // XXX debugging support - delete when done

	// Flags related to where to watch for data (inotify events).
	dataHomeDir    = flag.String("data-home-dir", "/var/spool", "directory pathname under which experiment data is created")
	extensions     = flagx.StringArray{".json"}
	experiment     = flag.String("experiment", "", "name of the experiment (e.g., ndt)")
	datatypes      = flagx.StringArray{}
	missedAge      = flag.Duration("missed-age", 3*time.Hour, "minimum duration since a file's last modification time before it is considered missed")
	missedInterval = flag.Duration("missed-interval", 30*time.Minute, "time interval between scans of filesystem for missed files")

	// Flags related to program's execution.
	genSchema = flag.Bool("schema", false, "generate schema files for each datatype")
	verbose   = flag.Bool("verbose", false, "enable verbose mode")

	errExtraArgs    = errors.New("extra arguments on the command line")
	errNoNode       = errors.New("must specify mlab-node-name")
	errNoBucket     = errors.New("must specify GCS bucket")
	errNoExperiment = errors.New("must specify experiment")
	errNoDatatype   = errors.New("must specify at least one datatype")
)

func init() {
	flag.Var(&schemaFiles, "schema-file", "schema for each datatype in the format <datatype>:<pathname>")
	flag.Var(&extensions, "extensions", "filename extensions to watch within <data-dir>/<experiment>")
	flag.Var(&datatypes, "datatype", "datatype(s) to watch within <data-dir>/<experiment>")
}

// main supports two modes of operation:
//   - A short-lived interactive mode, enabled by the -schema flag,
//     to create and upload schema files to GCS.
//   - A long-lived non-interactive mode to bundle and upload data to GCS.
func main() {
	log.SetFlags(log.Ltime)
	if err := parseAndValidateCLI(); err != nil {
		log.Panic(err)
	}
	// In verbose mode, enable verbose logging (mostly for debugging).
	if *verbose {
		watchdir.Verbose(vLogf)
		uploadbundle.Verbose(vLogf)
	}

	// Make sure that schema file we generate matches exactly the
	// standard columns we wrap the measurement data in.
	if err := validateStdColsSchema(); err != nil {
		log.Panic(err)
	}

	if *genSchema {
		if err := createTableSchemas(); err != nil {
			log.Panic(err)
		}
	} else {
		watchAndUpload()
	}
}

// parseAndValidateCLI parses and validates the command line.
func parseAndValidateCLI() error {
	// Note that extensions was declared as flags.StringArray{".json"}
	// so the usage message would show the right default value.
	// But we have to set it to nil before parsing the flags because
	// flagx.StringArray always appends to the array and there is no
	// way to remove an element from it.
	extensions = nil
	flag.Parse()
	if extensions == nil {
		extensions = []string{".json"}
	}
	if flag.NArg() != 0 {
		return errExtraArgs
	}
	if err := flagx.ArgsFromEnv(flag.CommandLine); err != nil {
		return fmt.Errorf("failed to get args from the environment: %w", err)
	}
	if !*genSchema {
		if *mlabNodeName == "" {
			return errNoNode
		}
		if *bucket == "" {
			return errNoBucket
		}
		if *experiment == "" {
			return errNoExperiment
		}
	}
	if len(datatypes) == 0 {
		return errNoDatatype
	}
	return validateSchemaFlags()
}

// watchAndUpload bundles individual JSON files into JSONL bundles and
// uploads the bundles to GCS.
func watchAndUpload() {
	// For each datatype, start a directory watcher and a bundle
	// uploader.
	watchEvents := []notify.Event{notify.InCloseWrite, notify.InMovedTo}
	wg := sync.WaitGroup{}
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()
	for _, datatype := range datatypes {
		wdClient, err := startWatcher(mainCtx, mainCancel, &wg, datatype, watchEvents)
		if err != nil {
			log.Panicf("failed to start directory watcher: %v", err)
		}
		if err := startUploader(mainCtx, mainCancel, &wg, datatype, wdClient); err != nil {
			log.Panicf("failed to start bundle uploader: %v", err)
		}
	}

	// If there's an unrecoverable error that causes channels
	// to close or if the main context is explicitly canceled, the
	// goroutines created in startWatcher() and startBundleUploader()
	// will terminate and the following Wait() returns.
	wg.Wait()
}

// startWatcher starts a directory watcher goroutine that watches the
// specified directory and notifies its client of new (and potentially
// missed) files.
func startWatcher(mainCtx context.Context, mainCancel context.CancelFunc, wg *sync.WaitGroup, datatype string, watchEvents []notify.Event) (*watchdir.WatchDir, error) {
	watchDir := filepath.Join(*dataHomeDir, *experiment, datatype)
	wdClient, err := watchdir.New(watchDir, extensions, watchEvents, *missedAge, *missedInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate watcher: %w", err)
	}

	wg.Add(1)
	go func(wdClient *watchdir.WatchDir) {
		defer mainCancel()
		wdClient.WatchAndNotify(mainCtx)
		wg.Done()
	}(wdClient)
	return wdClient, nil
}

// startUploader start a bundle uploader goroutine that bundles
// individual JSON files into JSONL bundle and uploads it to GCS.
func startUploader(mainCtx context.Context, mainCancel context.CancelFunc, wg *sync.WaitGroup, datatype string, wdClient *watchdir.WatchDir) error {
	// Parse the M-Lab hostname (which should be in one of the
	// following formats) into its constituent parts.
	// v1: <machine>.<site>.measuremen-lab.org
	// v2: <machine>.<site>.<project>.measurement-lab.org
	nameParts, err := host.Parse(*mlabNodeName)
	if err != nil {
		log.Panic(err)
	}

	gcsConf := uploadbundle.GCSConfig{
		Bucket:  *bucket,
		DataDir: filepath.Join(*gcsHomeDir, *experiment, datatype),
		BaseID:  fmt.Sprintf("%s-%s-%s-%s", datatype, nameParts.Machine, nameParts.Site, *experiment),
	}
	bundleConf := uploadbundle.BundleConfig{
		Datatype: datatype,
		DataDir:  filepath.Join(*dataHomeDir, *experiment, datatype),
		SizeMax:  *bundleSizeMax,
		AgeMax:   *bundleAgeMax,
		NoRm:     *bundleNoRm,
	}
	ubClient, err := uploadbundle.New(wdClient, gcsConf, bundleConf)
	if err != nil {
		return fmt.Errorf("failed to instantiate uploader: %w", err)
	}

	wg.Add(1)
	go func(ubClient *uploadbundle.UploadBundle) {
		defer mainCancel()
		// BundleAndUpload() runs forever unless somehow the
		// context is canceled or the channels it uses are closed.
		ubClient.BundleAndUpload(mainCtx)
		wg.Done()
	}(ubClient)
	return nil
}

// vLogf logs the given message if verbose mode is enabled.  Because the
// verbose mode is used mostly for debugging, messages are prefixed by
// "filename:line-number function()" printed in green and the message
// printed in blue for easier visual inspection.
func vLogf(format string, args ...interface{}) {
	ansicode := map[string]string{
		"green": "\033[00;32m",
		"blue":  "\033[00;34m",
		"end":   "\033[0m",
	}
	if !*verbose {
		return
	}
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
	idx := strings.LastIndex(details.Name(), ".")
	if idx == -1 {
		idx = 0
	} else {
		idx++
	}
	file = filepath.Base(file)
	a := []interface{}{ansicode["green"], file, line, details.Name()[idx:], ansicode["blue"]}
	a = append(a, args...)
	log.Printf("%s%s:%d: %s(): %s"+format+"%s", append(a, ansicode["end"])...)
}
