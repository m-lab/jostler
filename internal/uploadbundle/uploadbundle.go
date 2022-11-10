// Package uploadbundle bundles multiple JSON files on the local
// filesystem into a single JSON Lines (JSONL) file and uploads the bundle
// to Google Could Storage (GCS).
//
// The local files should:
//
//  1. Be in date subdirectories (<yyyy>/<mm>/<dd>) of a data directory
//     configured via BundleConfig.DataDir.
//  2. Have pathnames that conform to regexp `[^a-zA-Z0-9/:._-]` and
//     not start with dot ('.') or have consecutive dots.
//  3. In proper JSON format with ".json" extension.
//  4. Be smaller than the maximum size of a bundle (BundleConfig.SizeMax).
//
// Bundle objects uploaded to GCS follow this naming convention:
//
//	<BundleConfig.DataDir>/<yyyy>/<mm>/<dd>/<timestamp>-<BaseID>.jsonl.gz
package uploadbundle

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/watchdir"
)

// UploadBundle defines configuration options and other fields that are
// common to all instances of JSONL bundles (see jsonlBundle).
type UploadBundle struct {
	wdClient      watchdir.WatchDirClient // directory watcher that notifies us
	gcsConf       GCSConfig               // GCS configuration
	bundleConf    BundleConfig            // bundle configuration
	ageChan       chan *jsonlBundle       // notification channel for when bundle reaches maximum age
	activeBundles map[string]*jsonlBundle // bundles that are active
	uploadBundles map[string]struct{}     // bundles that are being uploaded or were uploaded
}

// GCSConfig defines GCS configuration options.
//
// BaseID is the ID component in the base name of
// the JSONL bundles and can be any string.  M-Lab uses
// "<datatype>-<machine>-<site>-<experiment>".  For example, the BaseID
// of bundle "20220914T143133.179976Z-foo1-mlab3-akl01-ndt.jsonl.gz" is
// "foo1-mlab3-akl01-ndt".
type GCSConfig struct {
	Bucket    string
	DataDir   string
	BaseID    string
	gcsClient gcs.GCSClient
}

// BundleConfig defines bundle configuration options.
type BundleConfig struct {
	Datatype  string        // datatype (e.g., ndt)
	DataDir   string        // path to datatype subdirectory on local disk (e.g., /cache/data/<experiment>/<datatype>)
	GoldenRow string        // datatype's golden row
	SizeMax   uint          // bundle will be uploaded when it reaches this size
	AgeMax    time.Duration // bundle will be uploaded when it reaches this age
	NoRm      bool          // XXX debugging support - delete when done
}

var (
	weekDays   = 7   // entries in the map
	numUploads = 100 // concurrent uploads

	ErrConfig       = errors.New("invalid configuration")
	ErrNotInDataDir = errors.New("is not in data directory")
	ErrTooShort     = errors.New("is too short")
	ErrInvalidChars = errors.New("has invalid characters")
	ErrDotDot       = errors.New("includes '..'")
	ErrDateDir      = errors.New("is not in .../yyyy/mm/dd/... format")
	ErrDotFile      = errors.New("starts with '.'")
	ErrNotRegular   = errors.New("is not a regular file")
	ErrEmpty        = errors.New("is empty")
	ErrTooBig       = errors.New("is too big to fit in a bundle")

	// Testing and debugging support.
	GCSClient = gcs.NewClient
	verbose   = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostfly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new UploadBundle instance.  Clients call this function
// for each datatype.
func New(ctx context.Context, wdClient watchdir.WatchDirClient, gcsConf GCSConfig, bundleConf BundleConfig) (*UploadBundle, error) {
	if wdClient == nil {
		return nil, fmt.Errorf("%w: nil watchdir client", ErrConfig)
	}
	if gcsConf.Bucket == "" || gcsConf.DataDir == "" || gcsConf.BaseID == "" || bundleConf.DataDir == "" {
		return nil, fmt.Errorf("%w: empty string in parameters", ErrConfig)
	}
	gcsClient, err := GCSClient(ctx, gcsConf.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	ub := &UploadBundle{
		wdClient:      wdClient,
		gcsConf:       gcsConf,
		bundleConf:    bundleConf,
		ageChan:       make(chan *jsonlBundle),
		activeBundles: make(map[string]*jsonlBundle, weekDays),
		uploadBundles: make(map[string]struct{}, numUploads),
	}
	ub.gcsConf.gcsClient = gcsClient
	ub.bundleConf.DataDir = filepath.Clean(ub.bundleConf.DataDir)
	return ub, nil
}

// BundleAndUpload continuously reads from two channels until its context
// is canceled.  One channel provides pathnames to new or potentially
// missed files that should be added to the bundle.  The other channel
// provides timer notifications for in-memory bundles that have reached
// their maximum age and should be uploaded to GCS.
func (ub *UploadBundle) BundleAndUpload(ctx context.Context) error {
	verbose("bundling and uploading files in %v", ub.bundleConf.DataDir)
	done := false
	for !done {
		select {
		case <-ctx.Done():
			verbose("'bundle and upload' context canceled for %v", ub.bundleConf.DataDir)
			done = true
		case watchEvent, chOpen := <-ub.wdClient.WatchChan():
			if !chOpen {
				verbose("watch channel closed")
				done = true
				break
			}
			// A new or missing JSON file was detected.
			ub.bundleFile(ctx, watchEvent.Path)
		case jb, chOpen := <-ub.ageChan:
			if !chOpen {
				verbose("age channel closed")
				done = true
				break
			}
			// A bundle reached its maximum age.
			ub.uploadAgedBundle(ctx, jb)
		}
	}
	return nil
}

// bundleFile adds the given file to a bundle if it's a valid JSON file and
// is has not been bundled before.
func (ub *UploadBundle) bundleFile(ctx context.Context, fullPath string) {
	// Validate the file's pathname and get its date subdirectory
	// and size.
	dateSubdir, fileSize, err := ub.fileDetails(fullPath)
	if err != nil {
		verbose("WARNING: ignoring %v: %v", fullPath, err)
		return
	}
	verbose("%v %v bytes", fullPath, fileSize)

	// Is there an active bundle that this file belongs to?
	jb := ub.activeBundles[dateSubdir]
	if jb != nil {
		// Sanity check.
		if jb.hasFile(fullPath) {
			log.Panicf("%v already in active %v", fullPath, jb.description())
		}
		// Check if there's enough room for this file in the active
		// bundle.  If not, upload this bundle and instantiate a
		// new one.
		if jb.size+uint(fileSize) > ub.bundleConf.SizeMax {
			verbose("not enough room in active %v for %v", jb.description(), fullPath)
			ub.uploadBundle(ctx, jb)
			jb = nil
		}
	}
	if jb == nil {
		jb = ub.newJSONLBundle(dateSubdir)
	}
	// Add the contents of this file to the bundle.
	if err := jb.addFile(fullPath); err != nil {
		log.Printf("ERROR: failed to add file to active bundle: %v\n", err)
	} else {
		verbose("active %v has %v bytes", jb.description(), jb.size)
	}
}

// newJSONLBundle creates and returns a new active bundle instance.
func (ub *UploadBundle) newJSONLBundle(dateSubdir string) *jsonlBundle {
	// Sanity check: make sure we don't already have a bundle for
	// the given date.
	if jb, ok := ub.activeBundles[dateSubdir]; ok {
		if dateSubdir == jb.dateSubdir {
			log.Panicf("an active %v already exists", jb.description())
		}
		log.Panicf("key %v returned active %v", dateSubdir, jb.description())
	}

	nowUTC := time.Now().UTC()
	objName := fmt.Sprintf("%s-%s", nowUTC.Format("20060102T150405.000000Z"), ub.gcsConf.BaseID)
	jb := &jsonlBundle{
		lines:      []string{},
		timestamp:  time.Now().UTC().Format("2006/01/02T150405.000000Z"),
		datatype:   ub.bundleConf.Datatype,
		dateSubdir: dateSubdir,
		bucket:     ub.gcsConf.Bucket,
		objDir:     fmt.Sprintf("%s/%s", ub.gcsConf.DataDir, nowUTC.Format("2006/01/02")), // e.g., ndt/pcap/2022/09/14
		objName:    objName + ".jsonl",
		idxName:    objName + ".index",
		fullPaths:  []string{},
		badFiles:   []string{},
		size:       0,
		noRm:       ub.bundleConf.NoRm,
	}
	ub.activeBundles[dateSubdir] = jb
	verbose("created active %v", jb.description())
	time.AfterFunc(ub.bundleConf.AgeMax, func() {
		ub.ageChan <- jb
	})
	log.Printf("started age timer to go off in %v for active %v\n", ub.bundleConf.AgeMax, jb.description())
	return jb
}

// fileDetails first verifies fullPath follows M-Lab's conventions
// /cache/data/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<filename>
// and is a regular file.  Then it makes sure it's not too big.
// If all is OK, it returns the date component of the file's pathname
// ("yyyy/mm/dd") and the file size.
func (ub *UploadBundle) fileDetails(fullPath string) (string, int64, error) {
	cleanFilePath := filepath.Clean(fullPath)
	dataDir := ub.bundleConf.DataDir
	if !strings.HasPrefix(cleanFilePath, dataDir) {
		return "", 0, fmt.Errorf("%v: %w", cleanFilePath, ErrNotInDataDir)
	}
	if len(cleanFilePath) <= len(dataDir) {
		return "", 0, fmt.Errorf("%v: %w", cleanFilePath, ErrTooShort)
	}
	pathName := regexp.MustCompile(`[^a-zA-Z0-9/:._-]`)
	if pathName.MatchString(cleanFilePath) {
		return "", 0, fmt.Errorf("%v: %w", cleanFilePath, ErrInvalidChars)
	}
	if strings.Contains(cleanFilePath, "..") {
		return "", 0, fmt.Errorf("%v: %w", cleanFilePath, ErrDotDot)
	}
	dateSubdir, filename := filepath.Split(cleanFilePath[len(dataDir):])
	yyyymmdd := regexp.MustCompile(`/20[0-9][0-9]/[0-9]{2}/[0-9]{2}/`)
	if len(dateSubdir) != 12 || !yyyymmdd.MatchString(dateSubdir) {
		return "", 0, fmt.Errorf("%v: %w", cleanFilePath, ErrDateDir)
	}
	if strings.HasPrefix(filename, ".") {
		return "", 0, fmt.Errorf("%v: %w", filename, ErrDotFile)
	}
	fi, err := os.Stat(fullPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to stat: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return "", 0, fmt.Errorf("%v: %w", filename, ErrNotRegular)
	}
	if uint(fi.Size()) == 0 {
		return "", 0, fmt.Errorf("%v: %w", filename, ErrEmpty)
	}
	if uint(fi.Size()) > ub.bundleConf.SizeMax {
		return "", 0, fmt.Errorf("%v: %w", filename, ErrTooBig)
	}
	return dateSubdir[1:11], fi.Size(), nil
}

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
