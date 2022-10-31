// Package main implements jostler.
//
// jostler supports two modes of operation:
//
//   - A short-lived interactive "local" mode meant to run on a user's
//     workstation to create table schema files in JSON format and
//     save them in the current directory so the user can easily examine
//     them (mostly for troubleshooting purposes).
//   - A long-lived non-interactive "daemon" mode meant to run on M-Lab
//     nodes to bundle and upload measurement data to GCS.
//
// When running in the non-interactive mode, jostler checks if the current
// table schema for each datatype ("new") is backward compatible with the
// datatypes's previous table schema ("old").  There are four possible
// scenarios with respect to old and new:
//
//  1. Old doesn't exists (i.e, this is the first time jostler is
//     invoked for the given datatype).
//  2. Old exists and matches new.
//  3. Old exists and doesn't match new, but new is backward compatible
//     with old.
//  4. Old exists and doesn't match new, and new isn't backward
//     compatible with old.
//
// Below is how jostler behaves under each of the above scenarios:
//
//  1. Uploads new to GCS.
//  2. Doesn't upload.
//  3. Uploads new to GCS.
//  4. Fails to run.
package main

import (
	"context"
	"errors"
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

	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

var (
	errWrite = errors.New("failed to write file")

	// Test code changes Fatal to Panic so a fatal error won't exit
	// the process and can be recovered.
	fatal = log.Fatal
)

func main() {
	// Change log's default output from stderr to stdout.
	// Otherwise, all messages will be treated as error!
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)
	if err := parseAndValidateCLI(); err != nil {
		fatal(err)
	}

	if local {
		if err := localMode(); err != nil {
			fatal(err)
		}
	} else {
		if err := daemonMode(); err != nil {
			fatal(err)
		}
	}
}

// localMode creates table schemas with standard columns for each datatype
// and saves them as <datatype>-schema.json files in the current directory
// so they can be easily examined by the user.
func localMode() error {
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, schemaFiles)
		tblSchemaJSON, err := schema.CreateTableSchemaJSON(datatype, dtSchemaFile)
		if err != nil {
			return fmt.Errorf("%v: %w", datatype, err)
		}
		schemaFile := datatype + "-schema.json"
		if err = os.WriteFile(schemaFile, tblSchemaJSON, 0o666); err != nil {
			return fmt.Errorf("%v: %w", errWrite, err)
		}
		log.Printf("saved %v\n", schemaFile)
	}
	return nil
}

// deamonMode runs in the long-lived non-interactive mode to bundle
// individual measurement data files in JSON format into JSONL bundles and
// upload to GCS.
func daemonMode() error {
	// Validate table schemas are backward compatibale and upload the
	// ones are a superset of the previous table.
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, schemaFiles)
		if err := schema.ValidateAndUpload(bucket, experiment, datatype, dtSchemaFile); err != nil {
			return fmt.Errorf("%v: %w", datatype, err)
		}
	}

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
	if testInterval.Seconds() != 0 {
		<-time.After(testInterval)
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
	watchDir := filepath.Join(dataHomeDir, experiment, datatype)
	wdClient, err := watchdir.New(watchDir, extensions, watchEvents, missedAge, missedInterval)
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
	nameParts, err := host.Parse(mlabNodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse hostname: %w", err)
	}

	gcsConf := uploadbundle.GCSConfig{
		Bucket:  bucket,
		DataDir: filepath.Join(gcsHomeDir, experiment, datatype),
		BaseID:  fmt.Sprintf("%s-%s-%s-%s", datatype, nameParts.Machine, nameParts.Site, experiment),
	}
	bundleConf := uploadbundle.BundleConfig{
		Datatype: datatype,
		DataDir:  filepath.Join(dataHomeDir, experiment, datatype),
		SizeMax:  bundleSizeMax,
		AgeMax:   bundleAgeMax,
		NoRm:     bundleNoRm,
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

const (
	ansiGreen  = "\033[00;32m"
	ansiBlue   = "\033[00;34m"
	ansiPurple = "\033[00;35m"
	ansiEnd    = "\033[0m"
)

// vLogf logs messages in verbose mode (mostly for debugging).  Messages
// are prefixed by "filename:line-number function()" printed in green and
// the message printed in blue for easier visual inspection.
func vLogf(format string, args ...interface{}) {
	if !verbose {
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
	file = filepath.Base(file)
	idx := strings.LastIndex(details.Name(), "/")
	if idx == -1 {
		idx = 0
	} else {
		idx++
	}
	a := []interface{}{ansiGreen, file, line, details.Name()[idx:], ansiBlue}
	a = append(a, args...)
	log.Printf("%s%s:%d: %s(): %s"+format+"%s", append(a, ansiEnd)...)
}
