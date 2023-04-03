// Package uploadbundle implements logic to bundle multiple JSON files
// on the local filesystem into JSONL bundles and upload them to Google
// Cloud Storage (GCS).
//
// The local files should:
//
//  1. Be in date subdirectories (<yyyy>/<mm>/<dd>) of a data directory
//     configured via BundleConfig.DataDir.
//  2. Have basenames conforming to regexp ^[a-zA-Z0-9][a-zA-Z0-9:._-]*.json
//     and not have consecutive dots.
//  3. In proper JSON format with ".json" extension.
//  4. Be smaller than the maximum size of a bundle (BundleConfig.SizeMax).
//
// GCS object names of JSONL bundles and their corresponding indices
// have the following format:
//
//	autoload/v1/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<timestamp>-<datatype>-<node>-<experiment>.jsonl.gz
//	|--------GCSConfig.DataDir--------|                                   |-------GCSConfig.BaseID-----|
//	autoload/v1/<experiment>/index1/<yyyy>/<mm>/<dd>/<timestamp>-<datatype>-<node>-<experiment>.index.gz
//	|------GCSConfig.IndexDir-----|                                   |-------GCSConfig.BaseID-----|
package uploadbundle

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"github.com/m-lab/jostler/internal/jsonlbundle"
	"github.com/m-lab/jostler/internal/watchdir"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DirWatcher defines the interface of a directory watcher.
type DirWatcher interface {
	WatchChan() chan watchdir.WatchEvent
	WatchAckChan() chan<- []string
	WatchAndNotify(context.Context) error
}

// UploadBundle defines configuration options and other fields that are
// common to all instances of JSONL bundles (see jsonlBundle).
type UploadBundle struct {
	wdClient      DirWatcher                              // directory watcher that notifies us
	gcsConf       GCSConfig                               // GCS configuration
	bundleConf    BundleConfig                            // bundle configuration
	ageChan       chan *jsonlbundle.JSONLBundle           // notification channel for when bundle reaches maximum age
	activeBundles map[civil.Date]*jsonlbundle.JSONLBundle // bundles that are active
	uploadBundles map[string]struct{}                     // bundles that are being uploaded or were uploaded
}

// Uploader interface.
type Uploader interface {
	Upload(context.Context, string, []byte) error
}

// GCSConfig defines GCS configuration options.
// Note that while slashes ("/") in GCS object names create the illusion
// of a directory hierarchy, GCS has a flat namesapce.
type GCSConfig struct {
	GCSClient Uploader
	Bucket    string // GCS bucket name
	DataDir   string // see the comment at the top of this file
	IndexDir  string // see the comment at the top of this file
	BaseID    string // see the comment at the top of this file
}

// BundleConfig defines bundle configuration options.
type BundleConfig struct {
	Version   string        // version of this program producing the bundle (e.g., v0.1.7)
	GitCommit string        // git commit SHA1 of this program (e.g., 2abe77f)
	Datatype  string        // datatype (e.g., scamper1)
	SpoolDir  string        // path to datatype subdirectory on local disk (e.g., /var/spool/<experiment>/<datatype>)
	SizeMax   uint          // bundle will be uploaded when it reaches this size
	AgeMax    time.Duration // bundle will be uploaded when it reaches this age
}

// Exported errors.
var (
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
	ErrDateParse    = errors.New("date unparseable")
)

var (
	weekDays   = 7   // entries in the map
	numUploads = 100 // concurrent uploads

	jostlerBytesPerBundle = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "jostler_bytes_per_bundle",
			Help:    "The number of bytes in each JSONL bundle jostler has uploaded",
			Buckets: []float64{1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8},
		},
		[]string{"datatype"})

	// Testing and debugging support.
	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new UploadBundle instance.  Clients call this function
// for each datatype.
func New(ctx context.Context, wdClient DirWatcher, gcsConf GCSConfig, bundleConf BundleConfig) (*UploadBundle, error) {
	if wdClient == nil || reflect.ValueOf(wdClient).IsNil() {
		return nil, fmt.Errorf("%w: nil watchdir client", ErrConfig)
	}
	if gcsConf.GCSClient == nil || gcsConf.Bucket == "" || gcsConf.DataDir == "" || gcsConf.BaseID == "" || bundleConf.SpoolDir == "" {
		return nil, fmt.Errorf("%w: nil or empty string in GCS configuration", ErrConfig)
	}
	ub := &UploadBundle{
		wdClient:      wdClient,
		gcsConf:       gcsConf,
		bundleConf:    bundleConf,
		ageChan:       make(chan *jsonlbundle.JSONLBundle),
		activeBundles: make(map[civil.Date]*jsonlbundle.JSONLBundle, weekDays),
		uploadBundles: make(map[string]struct{}, numUploads),
	}
	ub.bundleConf.SpoolDir = filepath.Clean(ub.bundleConf.SpoolDir)
	return ub, nil
}

// BundleAndUpload continuously reads from two channels until its context
// is canceled.  One channel provides pathnames to new or potentially
// missed files that should be added to the bundle.  The other channel
// provides timer notifications for in-memory bundles that have reached
// their maximum age and should be uploaded to GCS.
func (ub *UploadBundle) BundleAndUpload(ctx context.Context) error {
	verbose("bundling and uploading files in %v", ub.bundleConf.SpoolDir)
	done := false
	for !done {
		select {
		case <-ctx.Done():
			verbose("'bundle and upload' context canceled for %v", ub.bundleConf.SpoolDir)
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
	date, fileSize, err := ub.fileDetails(fullPath)
	if err != nil {
		verbose("WARNING: ignoring %v: %v", fullPath, err)
		return
	}
	verbose("%v %v bytes", fullPath, fileSize)

	// Is there an active bundle that this file belongs to?
	jb := ub.activeBundles[date]
	if jb != nil {
		// Sanity check.
		if jb.HasFile(fullPath) {
			log.Printf("INTERNAL ERROR: %v already in active %v", fullPath, jb.Description())
		}
		// Check if there's enough room for this file in the active
		// bundle.  If not, upload this bundle and instantiate a
		// new one.
		if jb.Size+uint(fileSize) > ub.bundleConf.SizeMax {
			verbose("not enough room in active %v for %v", jb.Description(), fullPath)
			ub.uploadBundle(ctx, jb)
			jb = nil
		}
	}
	if jb == nil {
		jb = ub.newJSONLBundle(date)
	}
	// Add the contents of this file to the bundle.
	if err := jb.AddFile(fullPath, ub.bundleConf.Version, ub.bundleConf.GitCommit); err != nil {
		log.Printf("ERROR: failed to add file to active bundle: %v\n", err)
	} else {
		verbose("active %v has %v bytes", jb.Description(), jb.Size)
	}
}

// fileDetails first verifies fullPath follows M-Lab's conventions
// /cache/data/<experiment>/<datatype>/<yyyy>/<mm>/<dd>/<filename>
// and is a regular file.  Then it makes sure it's not too big.
// If all is OK, it returns the date component of the file's pathname
// ("yyyy/mm/dd") and the file size.
func (ub *UploadBundle) fileDetails(fullPath string) (civil.Date, int64, error) {
	cleanFilePath := filepath.Clean(fullPath)
	dataDir := ub.bundleConf.SpoolDir
	if !strings.HasPrefix(cleanFilePath, dataDir) {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", cleanFilePath, ErrNotInDataDir)
	}
	if len(cleanFilePath) <= len(dataDir) {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", cleanFilePath, ErrTooShort)
	}
	pathName := regexp.MustCompile(`[^a-zA-Z0-9/:._-]`)
	if pathName.MatchString(cleanFilePath) {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", cleanFilePath, ErrInvalidChars)
	}
	if strings.Contains(cleanFilePath, "..") {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", cleanFilePath, ErrDotDot)
	}
	dateSubdir, filename := filepath.Split(cleanFilePath[len(dataDir):])
	yyyymmdd := regexp.MustCompile(`/20[0-9][0-9]/[0-9]{2}/[0-9]{2}/`)
	if len(dateSubdir) != 12 || !yyyymmdd.MatchString(dateSubdir) {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", cleanFilePath, ErrDateDir)
	}
	if strings.HasPrefix(filename, ".") {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", filename, ErrDotFile)
	}
	fi, err := os.Stat(fullPath)
	if err != nil {
		return civil.Date{}, 0, fmt.Errorf("failed to stat: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", filename, ErrNotRegular)
	}
	if uint(fi.Size()) == 0 {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", filename, ErrEmpty)
	}
	if uint(fi.Size()) > ub.bundleConf.SizeMax {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", filename, ErrTooBig)
	}
	date, err := civil.ParseDate(strings.ReplaceAll(dateSubdir[1:11], "/", "-"))
	if err != nil {
		return civil.Date{}, 0, fmt.Errorf("%v: %w", filename, ErrDateParse)
	}
	return date, fi.Size(), nil
}

// newJSONLBundle creates and returns a new active bundle instance.
func (ub *UploadBundle) newJSONLBundle(date civil.Date) *jsonlbundle.JSONLBundle {
	// Sanity check: make sure we don't already have a bundle for
	// the given date.
	if jb, ok := ub.activeBundles[date]; ok {
		if date == jb.Date {
			log.Printf("INTERNAL ERROR: an active %v already exists", jb.Description())
		}
		log.Printf("INTERNAL ERROR: key %s returned active %v", date, jb.Description())
	}

	jb := jsonlbundle.New(ub.gcsConf.Bucket, ub.gcsConf.DataDir, ub.gcsConf.IndexDir, ub.gcsConf.BaseID, ub.bundleConf.Datatype, date)
	ub.activeBundles[date] = jb
	verbose("created active %v", jb.Description())
	time.AfterFunc(ub.bundleConf.AgeMax, func() {
		ub.ageChan <- jb
	})
	log.Printf("started age timer to go off in %v for active %v\n", ub.bundleConf.AgeMax, jb.Description())
	return jb
}

// uploadAgedBundle uploads the given bundle if it is still active.
// Otherwise, we should delete it from the upload bundles map because
// we received its age timer.
func (ub *UploadBundle) uploadAgedBundle(ctx context.Context, jb *jsonlbundle.JSONLBundle) {
	verbose("age timer went off for %v", jb.Description())
	if _, ok := ub.uploadBundles[jb.Timestamp]; ok {
		verbose("%v is already uploaded or being uploaded now", jb.Description())
		delete(ub.uploadBundles, jb.Timestamp)
		return
	}
	ub.uploadBundle(ctx, jb)
}

// uploadBundle adds the given bundle (which should be active) to the
// upload bundles map, deletes it from the active bundles map, and start
// the uploads process to GCS in the background.
func (ub *UploadBundle) uploadBundle(ctx context.Context, jb *jsonlbundle.JSONLBundle) {
	// Sanity check.
	if _, ok := ub.activeBundles[jb.Date]; !ok {
		log.Printf("INTERNAL ERROR: %v not in active bundles map", jb.Description())
	}
	if len(jb.Lines) != len(jb.Index)+len(jb.BadFiles) {
		log.Printf("INTERNAL ERROR: %v != %v + %v", len(jb.Lines), len(jb.Index), len(jb.BadFiles))
	}

	// Add the bundle to upload bundles map.
	ub.uploadBundles[jb.Timestamp] = struct{}{}
	// Delete the bundle from active bundles map.
	delete(ub.activeBundles, jb.Date)

	// Start the upload process in the background and acknowledge
	// the files of this bundle with the directory watcher.
	go ub.uploadInBackground(ctx, jb, true)
}

// uploadInBackground starts the process of uploading the specified
// measurement data (JSONL bundle) and its associated index in the
// background.
func (ub *UploadBundle) uploadInBackground(ctx context.Context, jb *jsonlbundle.JSONLBundle, ack bool) {
	go func(jb *jsonlbundle.JSONLBundle) {
		var err error
		// Upload data bundle.
		objPath := filepath.Join(jb.BundleDir, jb.BundleName)
		contents := []byte(strings.Join(jb.Lines, "\n"))
		if err = gzipAndUpload(ctx, ub.gcsConf.GCSClient, objPath, jb.Datatype, contents); err != nil {
			log.Printf("ERROR: data bundle %v: %v\n", jb.Description(), err)
			return
		}

		// Upload index bundle.
		objPath = filepath.Join(jb.IndexDir, jb.IndexName)
		contents, err = jb.MarshalIndex()
		if err != nil {
			log.Printf("ERROR: failed to marshal index for bundle %v: %v\n", jb.Description(), err)
			return
		}
		if err = gzipAndUpload(ctx, ub.gcsConf.GCSClient, objPath, "index1", contents); err != nil {
			log.Printf("ERROR: index bundle %v: %v\n", jb.Description(), err)
			return
		}

		// Remove uploaded files from the local filesystem.
		jb.RemoveLocalFiles()

		// Tell directory watcher we're done with these files.
		if ack {
			ub.wdClient.WatchAckChan() <- append(jb.IndexFilenames(), jb.BadFiles...)
		}
	}(jb)
}

// gzipAndUpload compresses the specified contents and uploads it via
// the specified upload client.
func gzipAndUpload(ctx context.Context, gcsClient Uploader, objPath, datatype string, contents []byte) error {
	var gzContents bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzContents)
	if _, err := gzipWriter.Write(contents); err != nil {
		return fmt.Errorf("failed to gzip: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	verbose("uploading %v", objPath)
	gzBytes := gzContents.Bytes()
	if err := gcsClient.Upload(ctx, objPath, gzBytes); err != nil {
		return fmt.Errorf("failed to upload: %w", err)
	}
	jostlerBytesPerBundle.WithLabelValues(datatype).Observe(float64(len(gzBytes)))
	return nil
}
