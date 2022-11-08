package uploadbundle

import (
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"
)

// UploadActiveBundlesAndWait uploads all active bundles regardless
// of their age and size.  This is primarily meant to provide a graceful
// shutdown.
func (ub *UploadBundle) UploadActiveBundlesAndWait(ctx context.Context, ack, wait bool) {
	verbose("start uploading all active %v bundles", ub.bundleConf.Datatype)
	wg := sync.WaitGroup{}
	for _, jb := range ub.activeBundles {
		verbose("uploading active %v", jb.description())
		if wait {
			wg.Add(1)
		}
		ub.uploadInBackground(ctx, jb, ack)
	}
	if wait {
		verbose("waiting for uploads of active %v bundles to finish", ub.bundleConf.Datatype)
		wg.Wait()
		verbose("finished uploading all active %v bundles", ub.bundleConf.Datatype)
	} else {
		verbose("not waiting for %v bundle uploads to finish", ub.bundleConf.Datatype)
	}
}

// uploadAgedBundle uploads the given bundle if it is still active.
// Otherwise, we should delete it from the upload bundles map because
// we received its age timer.
func (ub *UploadBundle) uploadAgedBundle(ctx context.Context, jb *jsonlBundle) {
	verbose("age timer went off for %v", jb.description())
	if _, ok := ub.uploadBundles[jb.timestamp]; ok {
		verbose("%v is already uploaded or being uploaded now", jb.description())
		delete(ub.uploadBundles, jb.timestamp)
		return
	}
	ub.uploadBundle(ctx, jb)
}

// uploadBundle adds the given bundle (which should be active) to the
// upload bundles map, deletes it from the active bundles map, and start
// the uploads process to GCS in the background.
func (ub *UploadBundle) uploadBundle(ctx context.Context, jb *jsonlBundle) {
	// Sanity check.
	if _, ok := ub.activeBundles[jb.dateSubdir]; !ok {
		log.Panicf("%v not in active bundles map", jb.description())
	}

	// Add the bundle to upload bundles map.
	ub.uploadBundles[jb.timestamp] = struct{}{}
	// Delete the bundle from active bundles map.
	delete(ub.activeBundles, jb.dateSubdir)

	// Start the upload process in the background and acknowledge
	// the files of this bundle with the directory watcher.
	go ub.uploadInBackground(ctx, jb, true)
}

// upload starts the process of uploading the specified measurement data
// (JSONL bundle) and its associated index in the background.
func (ub *UploadBundle) uploadInBackground(ctx context.Context, jb *jsonlBundle, ack bool) {
	gcsClient := ub.gcsConf.gcsClient
	go func(jb *jsonlBundle) {
		// Upload the bundle.
		objPath := filepath.Join(jb.objDir, jb.objName)
		contents := []byte(strings.Join(jb.lines, "\n"))
		if err := gcsClient.Upload(ctx, objPath, contents); err != nil {
			log.Printf("ERROR: failed to upload bundle %v: %v\n", jb.description(), err)
			return
		}
		objPath = filepath.Join(jb.objDir, jb.idxName)
		contents = []byte(strings.Join(jb.fullPaths, "\n"))
		if err := gcsClient.Upload(ctx, objPath, contents); err != nil {
			log.Printf("ERROR: failed to upload index for bundle %v: %v\n", jb.description(), err)
			return
		}
		if jb.noRm {
			// XXX debugging support - delete when done.
			verbose("not removing files and index of %v", jb.description())
		} else {
			// Remove uploaded files from the local filesystem.
			jb.removeFiles()
		}
		// Tell directory watcher we're done with these files.
		if ack {
			ub.wdClient.WatchAckChan() <- append(jb.fullPaths, jb.badFiles...)
		}
	}(jb)
}
