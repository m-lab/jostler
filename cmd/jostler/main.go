// Package main implements jostler.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/host"
	"github.com/rjeczalik/notify"

	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

// Test code changes Fatal to Panic so a fatal error won't exit
// the process and can be recovered.
var fatal = log.Fatal

// main supports two modes of operation:
//   - A short-lived interactive mode, enabled by the -schema flag,
//     to create and upload schema files to GCS.
//   - A long-lived non-interactive mode to bundle and upload data to GCS.
func main() {
	// Change log's default output from stderr to stdout.
	// Otherwise, all messages will be treated as error!
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)
	if err := parseAndValidateCLI(); err != nil {
		fatal(err)
	}
	// In verbose mode, enable verbose logging (mostly for debugging).
	if *verbose {
		watchdir.Verbose(vLogf)
		uploadbundle.Verbose(vLogf)
	}

	// Make sure our template for table schemas matches exactly the
	// schema of the standard columns.
	if err := validateStdColsTemplate(); err != nil {
		fatal(err)
	}

	if *genSchema {
		// Short-lived interactive mode.
		if err := createTableSchemas(); err != nil {
			fatal(err)
		}
	} else {
		// Long-lived non-interactive mode.
		if err := watchAndUpload(); err != nil {
			fatal(err)
		}
	}
}

// watchAndUpload bundles individual JSON files into JSONL bundles and
// uploads the bundles to GCS.
func watchAndUpload() error {
	watchEvents := []notify.Event{notify.InCloseWrite, notify.InMovedTo}
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()
	wg := sync.WaitGroup{}
	// For each datatype, start a directory watcher and a bundle
	// uploader.
	for _, datatype := range datatypes {
		wdClient, err := startWatcher(mainCtx, mainCancel, &wg, datatype, watchEvents)
		if err != nil {
			return err
		}
		if _, err = startUploader(mainCtx, mainCancel, &wg, datatype, wdClient); err != nil {
			return err
		}
	}

	// When testing, we set testInterval to a non-zero value (e.g.,
	// 3 seconds) after which we cancel the main context to wrap up
	// and return.
	if testInterval.Abs() != 0 {
		<-time.After(*testInterval)
		mainCancel()
	}

	// If there's an unrecoverable error that causes channels
	// to close or if the main context is explicitly canceled, the
	// goroutines created in startWatcher() and startBundleUploader()
	// will terminate and the following Wait() returns.
	wg.Wait()
	return nil
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
func startUploader(mainCtx context.Context, mainCancel context.CancelFunc, wg *sync.WaitGroup, datatype string, wdClient *watchdir.WatchDir) (*uploadbundle.UploadBundle, error) {
	nameParts, err := host.Parse(*mlabNodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse hostname: %w", err)
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
		return nil, fmt.Errorf("failed to instantiate uploader: %w", err)
	}

	wg.Add(1)
	go func(ubClient *uploadbundle.UploadBundle) {
		defer mainCancel()
		// BundleAndUpload() runs forever unless somehow the
		// context is canceled or the channels it uses are closed.
		ubClient.BundleAndUpload(mainCtx)
		wg.Done()
	}(ubClient)
	return ubClient, nil
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
