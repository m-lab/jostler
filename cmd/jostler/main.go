// Package main implements jostler.  See README.md for a detailed
// description of how jostler works.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/m-lab/go/host"
	"github.com/rjeczalik/notify"

	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/schema"
	"github.com/m-lab/jostler/internal/testhelper"
	"github.com/m-lab/jostler/internal/uploadbundle"
	"github.com/m-lab/jostler/internal/watchdir"
)

var (
	version   string // set at build time from git describe --tags
	gitCommit string // set at build time from git log -1 --format=%h

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
	schema.LocalDataDir = localDataDir
	schema.GCSDataDir = gcsDataDir

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
// and saves them as <datatype>-table.json files in the current directory
// so they can be easily examined by the user.
func localMode() error {
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, dtSchemaFiles)
		tblSchemaJSON, err := schema.CreateTableSchemaJSON(datatype, dtSchemaFile)
		if err != nil {
			return fmt.Errorf("%v: %w", datatype, err)
		}
		tblSchemaFile := datatype + "-table.json"
		if err = os.WriteFile(tblSchemaFile, tblSchemaJSON, 0o666); err != nil {
			return fmt.Errorf("%v: %w", errWrite, err)
		}
		log.Printf("saved %v\n", tblSchemaFile)
	}
	return nil
}

// deamonMode runs in the long-lived non-interactive mode to bundle
// individual measurement data files in JSON format into JSONL bundles and
// upload to GCS.
func daemonMode() error {
	mainCtx, mainCancel := context.WithCancel(context.Background())
	// Create a storage client.
	// The gcsLocalDisk flag is meant for e2e testing where we want to read
	// from and write to the local disk storage instead of cloud storage.
	var stClient schema.DownloaderUploader
	var err error
	if gcsLocalDisk {
		stClient, err = testhelper.NewClient(mainCtx, bucket)
	} else {
		stClient, err = gcs.NewClient(mainCtx, bucket)
	}
	if err != nil {
		mainCancel()
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Validate table schemas are backward compatible and upload the
	// ones are a superset of the previous table.
	for _, datatype := range datatypes {
		dtSchemaFile := schema.PathForDatatype(datatype, dtSchemaFiles)
		if err = schema.ValidateAndUpload(stClient, bucket, experiment, datatype, dtSchemaFile); err != nil {
			mainCancel()
			return fmt.Errorf("%v: %w", datatype, err)
		}
	}

	// For each datatype, start a directory watcher and a bundle
	// uploader.
	watchEvents := []notify.Event{notify.InCloseWrite, notify.InMovedTo}
	watcherStatus := make(chan error)
	uploaderStatus := make(chan error)
	for _, datatype := range datatypes {
		var wdClient *watchdir.WatchDir
		wdClient, err = startWatcher(mainCtx, mainCancel, watcherStatus, datatype, watchEvents)
		if err != nil {
			return err
		}
		if _, err = startUploader(mainCtx, mainCancel, uploaderStatus, datatype, wdClient); err != nil {
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
	select {
	case err = <-watcherStatus:
	case err = <-uploaderStatus:
	}
	mainCancel()
	return err
}

// startWatcher starts a directory watcher goroutine that watches the
// specified directory and notifies its client of new (and potentially
// missed) files.
func startWatcher(mainCtx context.Context, mainCancel context.CancelFunc, status chan<- error, datatype string, watchEvents []notify.Event) (*watchdir.WatchDir, error) {
	watchDir := filepath.Join(localDataDir, experiment, datatype)
	// Create the directory to watch if it doesn't already exist.
	if err := os.MkdirAll(watchDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	wdClient, err := watchdir.New(watchDir, extensions, watchEvents, missedAge, missedInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate watcher: %w", err)
	}

	go func(wdClient *watchdir.WatchDir, status chan<- error) {
		defer mainCancel()
		status <- wdClient.WatchAndNotify(mainCtx)
	}(wdClient, status)
	return wdClient, nil
}

// startUploader start a bundle uploader goroutine that bundles
// individual JSON files into JSONL bundle and uploads it to GCS.
func startUploader(mainCtx context.Context, mainCancel context.CancelFunc, status chan<- error, datatype string, wdClient *watchdir.WatchDir) (*uploadbundle.UploadBundle, error) {
	nameParts, err := host.Parse(mlabNodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse hostname: %w", err)
	}

	// Create a storage client.
	// The gcsLocalDisk flag is meant for e2e testing where we want to read
	// from and write to the local disk storage instead of cloud storage.
	var stClient uploadbundle.Uploader
	if gcsLocalDisk {
		stClient, err = testhelper.NewClient(mainCtx, bucket)
	} else {
		stClient, err = gcs.NewClient(mainCtx, bucket)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	gcsConf := uploadbundle.GCSConfig{
		GCSClient: stClient,
		Bucket:    bucket,
		DataDir:   filepath.Join(gcsDataDir, experiment, datatype),
		IndexDir:  filepath.Join(gcsDataDir, experiment, "index1"),
		BaseID:    fmt.Sprintf("%s-%s-%s-%s", datatype, nameParts.Machine, nameParts.Site, experiment),
	}
	bundleConf := uploadbundle.BundleConfig{
		Version:   version,
		GitCommit: gitCommit,
		Datatype:  datatype,
		SpoolDir:  filepath.Join(localDataDir, experiment, datatype),
		SizeMax:   bundleSizeMax,
		AgeMax:    bundleAgeMax,
	}
	ubClient, err := uploadbundle.New(mainCtx, wdClient, gcsConf, bundleConf)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate uploader: %w", err)
	}

	go func(ubClient *uploadbundle.UploadBundle, status chan<- error) {
		defer mainCancel()
		// BundleAndUpload() runs forever unless somehow the
		// context is canceled or the channels it uses are closed.
		status <- ubClient.BundleAndUpload(mainCtx)
	}(ubClient, status)
	return ubClient, nil
}
