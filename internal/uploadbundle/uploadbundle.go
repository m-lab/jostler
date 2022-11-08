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
	"path/filepath"
	"time"

	"github.com/m-lab/jostler/internal/gcs"
	"github.com/m-lab/jostler/internal/watchdir"
)

// UploadBundle defines configuration options and other fields that are
// common to all instances of JSONL bundles (see jsonlBundle).
type UploadBundle struct {
	wdClient      *watchdir.WatchDir      // directory watcher that notifies us
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
func New(ctx context.Context, wdClient *watchdir.WatchDir, gcsConf GCSConfig, bundleConf BundleConfig) (*UploadBundle, error) {
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
