// Package main implements jostler.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/jostler/uploadbundle"
	"github.com/m-lab/jostler/watchdir"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/host"
	"github.com/rjeczalik/notify"
)

var (
	// Flags related to GCS.
	bucket       = flag.String("gcs-bucket", "", "required: GCS bucket name")
	gcsHomeDir   = flag.String("gcs-home-dir", "autoload-v0", "home directory in GCS bucket under which bundles will be uploaded")
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
	missedAgeMin   = flag.Duration("missed-age-min", 3*time.Hour, "minimum duration since a file's last modification time before it is considered missed")
	missedInterval = flag.Duration("missed-interval", 30*time.Minute, "time interval between scans of filesystem for missed files")

	// Flags related to program's execution.
	verbose = flag.Bool("verbose", false, "enable verbose mode")
)

func init() {
	flag.Var(&goldenRows, "golden-row", "required: golden row for each datatype in the format <datatype>:<pathname>")
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

	// Parse the host name which should be a string similar to
	// mlab4.abc0t.measurement-lab.org into its components.
	node, err := host.Parse(*mlabNodeName)
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
		goldenRow, err := validateGoldenRow(datatype)
		if err != nil {
			log.Panicf("invalid golden row: %v", err)
		}
		if err := startBundleUploader(&wg, datatype, goldenRow, node, wdClient); err != nil {
			log.Panicf("failed to start bundle uploader: %v", err)
		}
	}
	// Wait until all goroutines have terminated which, unless there
	// is an error, should never happen.
	wg.Wait()
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
		return fmt.Errorf("invalid command line: extra arguments")
	}
	if err := flagx.ArgsFromEnv(flag.CommandLine); err != nil {
		return fmt.Errorf("failed to get args from the environment: %v", err)
	}
	if *mlabNodeName == "" {
		return fmt.Errorf("must specify mlab-node-name")
	}
	if len(goldenRows) == 0 {
		return fmt.Errorf("must specify golden row per datatype")
	}
	if *bucket == "" {
		return fmt.Errorf("must specify GCS bucket")
	}
	if *experiment == "" {
		return fmt.Errorf("must specify experiment")
	}
	if len(datatypes) == 0 {
		return fmt.Errorf("must specify at least one datatype")
	}
	if len(goldenRows) != len(datatypes) {
		return fmt.Errorf("mismatch between the number of golden row(s) and datatype(s)")
	}
	return nil
}

// validateGoldenRow validates the golden row files specified
// via the command line for each datatype.  For example, for
// datatype foo1, it would be: foo1:</path/to>/<foo1-golden-row.json>.
func validateGoldenRow(datatype string) (string, error) {
	gRowFile := ""
	for _, gRow := range goldenRows {
		if strings.HasPrefix(gRow, datatype+":") {
			gRowFile = gRow[len(datatype)+1:]
			break
		}
	}
	if gRowFile == "" {
		return "", fmt.Errorf("failed to find golden row for datatype %v", datatype)
	}

	var gRowBytes []byte
	gRowBytes, err := os.ReadFile(gRowFile)
	if err != nil {
		return "", err
	}
	if !json.Valid(gRowBytes) {
		return "", fmt.Errorf("failed to validate golden row for datatype %v", datatype)
	}
	return strings.TrimSuffix(string(gRowBytes), "\n"), nil
}

// startWatcher starts a directory watcher goroutine that watches the
// specified directory and notfies its client of new (and potentially
// missed)files.
func startWatcher(wg *sync.WaitGroup, datatype string, watchEvents []notify.Event) (*watchdir.WatchDir, error) {
	watchDir := filepath.Join(*dataHomeDir, *experiment, datatype)
	wdClient, err := watchdir.New(watchDir, extensions, watchEvents, *missedAgeMin, *missedInterval)
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

// startBundleUploader start a bundle uploader goroutine that bundles
// individual JSON files into JSONL bundle and uploads it to GCS.
func startBundleUploader(wg *sync.WaitGroup, datatype, goldenRow string, node host.Name, wdClient *watchdir.WatchDir) error {
	gcsConf := uploadbundle.GCSConfig{
		Bucket:  *bucket,
		DataDir: filepath.Join(*gcsHomeDir, *experiment, datatype),
		BaseID:  fmt.Sprintf("%s-%s-%s-%s", datatype, node.Machine, node.Site, *experiment),
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
	go func(wdClient *watchdir.WatchDir) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ub.BundleAndUpload(ctx)
		vLogf("BundleAndUpload() returned")
		wg.Done()
	}(wdClient)
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
	i := strings.LastIndex(details.Name(), ".")
	if i == -1 {
		i = 0
	} else {
		i++
	}
	file = filepath.Base(file)
	a := []interface{}{ansicode["green"], file, line, details.Name()[i:], ansicode["blue"]}
	a = append(a, args...)
	log.Printf("%s%s:%d: %s(): %s"+format+"%s", append(a, ansicode["end"])...)
}
