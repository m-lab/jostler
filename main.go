// Package main implements jostler.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/host"
	"github.com/rjeczalik/notify"

	"github.com/m-lab/jostler/uploadbundle"
	"github.com/m-lab/jostler/watchdir"
)

var (
	// Flags related to GCS.
	bucket       = flag.String("gcs-bucket", "", "required: GCS bucket name")
	gcsHomeDir   = flag.String("gcs-home-dir", "autoload/v0", "home directory in GCS bucket under which bundles will be uploaded")
	mlabNodeName = flag.String("mlab-node-name", "", "required: node name specified directly or via MLAB_NODE_NAME env variable")

	// Flags related to bundles.
	goldenRows    = flagx.StringArray{}
	bundleSizeMax = flag.Uint("bundle-size-max", 20*1024*1024, "maximum size in bytes of a bundle before it is uploaded")
	bundleAgeMax  = flag.Duration("bundle-age-max", 1*time.Hour, "maximum age of a bundle before it is uploaded")
	bundleNoRm    = flag.Bool("no-rm", false, "do not remove files of a bundle after successful upload") // XXX debugging support - delete when done

	// Flags related to where to watch for data (inotify events).
	dataHomeDir    = flag.String("data-home-dir", "/cache/data", "directory pathname under which experiment data is created")
	extensions     = flagx.StringArray{".json"}
	experiment     = flag.String("experiment", "", "required: name of the experiment (e.g., ndt)")
	datatypes      = flagx.StringArray{} // required
	missedAge      = flag.Duration("missed-age", 3*time.Hour, "minimum duration since a file's last modification time before it is considered missed")
	missedInterval = flag.Duration("missed-interval", 30*time.Minute, "time interval between scans of filesystem for missed files")

	// Flags related to program's execution.
	schema  = flag.Bool("schema", false, "create schema files for each datatype")
	verbose = flag.Bool("verbose", false, "enable verbose mode")

	errExtraArgs     = errors.New("extra arguments on the command line")
	errNoNode        = errors.New("must specify mlab-node-name")
	errNoBucket      = errors.New("must specify GCS bucket")
	errNoExperiment  = errors.New("must specify experiment")
	errNoDatatype    = errors.New("must specify at least one datatype")
	errMismatch      = errors.New("mismatch between the number of golden row(s) and datatype(s)")
	errGRowArgFormat = errors.New("is not in <datatype>:<pathname> format")
	errNoGRowFile    = errors.New("failed to find golden row")
	errInvalidGRow   = errors.New("failed to validate golden row")
	errStatFile      = errors.New("failed to stat file")
	errReadFile      = errors.New("failed to read file")
)

func init() {
	flag.Var(&goldenRows, "golden-row", "golden row for each datatype in the format <datatype>:<pathname>")
	flag.Var(&extensions, "extensions", "filename extensions to watch within <data-dir>/<experiment>")
	flag.Var(&datatypes, "datatype", "required: datatype(s) to watch within <data-dir>/<experiment>")
}

func main() {
	log.SetFlags(log.Ltime)
	if err := parseAndValidateCLI(); err != nil {
		log.Panicf("invalid command line: %v", err)
	}
	if *verbose {
		watchdir.Verbose(vLogf)
		uploadbundle.Verbose(vLogf)
	}

	if *schema {
		createSchemas()
	} else {
		uploadBundles()
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
	if !*schema {
		if *mlabNodeName == "" {
			return errNoNode
		}
		if *bucket == "" {
			return errNoBucket
		}
	}
	if *experiment == "" {
		return errNoExperiment
	}
	if len(datatypes) == 0 {
		return errNoDatatype
	}
	if len(goldenRows) > len(datatypes) {
		return errMismatch
	}
	return nil
}

// createSchemas creates schema files for each datatype.  These schema files
// will be used to create BigQuery tables.
func createSchemas() {
	for _, datatype := range datatypes {
		_, err := goldenRowForDatatype(datatype)
		if err != nil {
			log.Panicf("invalid golden row: %v", err)
		}
	}
}

// uploadBundles uploads JSONL bundles from M-Lab nodes to GCS.  These JSONL
// bundles will be loaded to BigQuery.
func uploadBundles() {
	// Parse the M-Lab hostname (which should be one of the following
	// formats) into its constituent parts.
	// v1: <machine>.<site>.measuremen-lab.org
	// v2: <machine>.<site>.<project>.measurement-lab.org
	nameParts, err := host.Parse(*mlabNodeName)
	if err != nil {
		log.Panic(err)
	}

	// For each datatype, start a directory watcher and a bundle
	// uploader.
	watchEvents := []notify.Event{notify.InCloseWrite, notify.InMovedTo}
	wg := sync.WaitGroup{}
	for _, datatype := range datatypes {
		wdClient, err := startWatcher(&wg, datatype, watchEvents)
		if err != nil {
			log.Panicf("failed to start directory watcher: %v", err)
		}
		goldenRow, err := goldenRowForDatatype(datatype)
		if err != nil {
			log.Panicf("invalid golden row: %v", err)
		}
		if err := startBundleUploader(&wg, datatype, goldenRow, nameParts, wdClient); err != nil {
			log.Panicf("failed to start bundle uploader: %v", err)
		}
	}
	// If there's an unrecoverable error that causes channels
	// to close or if the main context is explicitly canceled, the
	// goroutines created in startWatcher() and startBundleUploader()
	// will terminate and the following Wait() returns.
	wg.Wait()
}

// goldenRowForDatatype validates the golden row file for the given datatype.
// By default are in /var/spool/<experiment>/<datatype>/golden-row.json
// but can also be specified via the --golden-row flag.  For example,
// for datatype foo1, it can be: foo1:<path>/<to>/<foo1-golden-row.json>.
func goldenRowForDatatype(datatype string) (string, error) {
	gRowFile := ""
	for _, gRow := range goldenRows {
		if !strings.Contains(gRow, ":") {
			return "", fmt.Errorf("%v: %w", gRow, errGRowArgFormat)
		}
		if strings.HasPrefix(gRow, datatype+":") {
			gRowFile = gRow[len(datatype)+1:]
			break
		}
	}
	if gRowFile == "" {
		gRowFile = filepath.Join("/var/spool", *experiment, datatype, "golden-row.json")
	}
	vLogf("checking golden row file %v for datatype %v", gRowFile, datatype)
	if _, err := os.Stat(gRowFile); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%v: %w", datatype, errNoGRowFile)
		}
		return "", fmt.Errorf("%v: %w", errStatFile, err)
	}

	var gRowBytes []byte
	gRowBytes, err := os.ReadFile(gRowFile)
	if err != nil {
		return "", fmt.Errorf("%v: %w", errReadFile, err)
	}
	if err := json.Unmarshal(gRowBytes, &struct{}{}); err != nil {
		return "", fmt.Errorf("%v: %v: %w", datatype, errInvalidGRow, err)
	}
	return strings.TrimSuffix(string(gRowBytes), "\n"), nil
}

// startWatcher starts a directory watcher goroutine that watches the
// specified directory and notifies its client of new (and potentially
// missed) files.
func startWatcher(wg *sync.WaitGroup, datatype string, watchEvents []notify.Event) (*watchdir.WatchDir, error) {
	watchDir := filepath.Join(*dataHomeDir, *experiment, datatype)
	wdClient, err := watchdir.New(watchDir, extensions, watchEvents, *missedAge, *missedInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate watcher: %w", err)
	}

	wg.Add(1)
	go func(wdClient *watchdir.WatchDir) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wdClient.WatchAndNotify(ctx)
		vLogf("WatchAndNotify() returned")
		wg.Done()
	}(wdClient)
	return wdClient, nil
}

// startBundleUploader starts a bundle uploader goroutine that bundles
// individual JSON files into JSONL bundles and uploads them to GCS.
func startBundleUploader(wg *sync.WaitGroup, datatype, goldenRow string, nameParts host.Name, wdClient *watchdir.WatchDir) error {
	gcsConf := uploadbundle.GCSConfig{
		Bucket:  *bucket,
		DataDir: filepath.Join(*gcsHomeDir, *experiment, datatype),
		BaseID:  fmt.Sprintf("%s-%s-%s-%s", datatype, nameParts.Machine, nameParts.Site, *experiment),
	}
	bundleConf := uploadbundle.BundleConfig{
		DataDir:   filepath.Join(*dataHomeDir, *experiment, datatype),
		GoldenRow: goldenRow,
		SizeMax:   *bundleSizeMax,
		AgeMax:    *bundleAgeMax,
		NoRm:      *bundleNoRm,
	}
	ub, err := uploadbundle.New(wdClient, gcsConf, bundleConf)
	if err != nil {
		return fmt.Errorf("failed to instantiate uploader: %w", err)
	}

	wg.Add(1)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ub.BundleAndUpload(ctx)
		vLogf("BundleAndUpload() returned")
		wg.Done()
	}()
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
