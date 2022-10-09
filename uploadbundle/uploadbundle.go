// Package uploadbundle bundles multiple JSON files on the local
// filesystem into a single JSON Lines (JSONL) file and uploads the bundle
// to Google Cloud Storage (GCS).
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
// Bundle objects uploaded to GCS follow this naming convention:
//
//	<BundleConfig.DataDir>/<yyyy>/<mm>/<dd>/<timestamp>-<BaseID>.jsonl.gz
package uploadbundle

import (
	"context"
	"path/filepath"
	"time"

	"github.com/m-lab/jostler/watchdir"
)

// UploadBundle defines configuration options and other fields that are
// common to all instances of JSONL bundles (see jsonlBundle).
type UploadBundle struct {
	wdClient      *watchdir.WatchDir  // directory watcher that notifies us
	gcsConf       GCSConfig           // GCS configuration
	bundleConf    BundleConfig        // bundle configuration
	uploadBundles map[string]struct{} // bundles that are being uploaded or were uploaded
}

// GCSConfig defines GCS configuration options.
//
// BaseID is the ID component in the base name of
// the JSONL bundles and can be any string.  M-Lab uses
// "<datatype>-<machine>-<site>-<experiment>".  For example, the BaseID
// of bundle "20220914T143133.179976Z-foo1-mlab3-akl01-ndt.jsonl.gz" is
// "foo1-mlab3-akl01-ndt".
type GCSConfig struct {
	Bucket  string // GCS bucket name
	DataDir string // "path" to datatype subdirectory in GCS (e.g., /autoload-v0/<experiment>/<datatype>)
	BaseID  string // ID component in the filename of JSONL bundle (e.g., <datatype>-<machine>-<site>-<experiment>)
}

// BundleConfig defines bundle configuration options.
type BundleConfig struct {
	DataDir   string        // path to datatype subdirectory on local disk (e.g., /cache/data/<experiment>/<datatype>)
	GoldenRow string        // datatype's golden row
	SizeMax   uint          // bundle will be uploaded when it reaches this size
	AgeMax    time.Duration // bundle will be uploaded when it reaches this age
	NoRm      bool          // XXX debugging support - delete when done
}

var verbose = func(fmt string, args ...interface{}) {}

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new UploadBundle instance.  Clients call this function
// for each datatype.
func New(wdClient *watchdir.WatchDir, gcsConf GCSConfig, bundleConf BundleConfig) (*UploadBundle, error) {
	ub := &UploadBundle{
		wdClient:      wdClient,
		gcsConf:       gcsConf,
		bundleConf:    bundleConf,
		uploadBundles: make(map[string]struct{}, 1000),
	}
	ub.bundleConf.DataDir = filepath.Clean(ub.bundleConf.DataDir)
	return ub, nil
}

// BundleAndUpload is a stub function.
func (ub *UploadBundle) BundleAndUpload(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// We are all done.
			verbose("context canceled; returning")
			return
		}
	}
}
