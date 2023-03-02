// Package jsonlbundle implements logic to process a single JSONL bundle.
package jsonlbundle

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/m-lab/jostler/api"
)

// JSONLBundle defines a collection of JSON file contents (i.e.,
// measurement data) bundled together in JSONL format for archiving.
// There is an index bundle associated with each measurement data bundle.
// See api/index.go for details about index bundles.
type JSONLBundle struct {
	Lines      []string      // contents of data files in the bundle
	BadFiles   []string      // pathnames of data files that could not be read or were not proper JSON
	Index      []api.IndexV1 // pathnames of data files in the index
	Timestamp  string        // bundle's in-memory creation time that serves as its identifier
	Datatype   string        // bundle's datatype
	DateSubdir string        // date subdirectory of files in this bundle (yyyy/mm/dd)
	bucket     string        // GCS bucket
	BundleDir  string        // GCS directory to upload this bundle to
	BundleName string        // GCS object name of this bundle
	IndexDir   string        // GCS directory to upload this bundle's index to
	IndexName  string        // GCS object name of this bundle's index
	Size       uint          // size of this bundle
}

// Exported errors.
var (
	ErrReadFile       = errors.New("failed to read file")
	ErrEmptyFile      = errors.New("empty file")
	ErrInvalidJSON    = errors.New("failed to validate JSON")
	ErrNotOneLine     = errors.New("is not one line")
	ErrMarshalStdCols = errors.New("failed to marshal standard columns")
	ErrMarshalIndex   = errors.New("failed to marshal index")
)

// Testing and debugging support.
var verbose = func(fmt string, args ...interface{}) {}

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new instance of JSONLBundle.
//
// GCS object names of data bundles and index bundles follow the
// following formats:
//
//	autoload/v1/<experiment>/<datatype>/date=<yyyy>-<mm>-<dd>/<timestamp>-<datatype>-<node>-<experiment>-data.jsonl
//	|--------GCSConfig.DataDir--------|                                   |------GCSConfig.BaseID------|
//	autoload/v1/<experiment>/index1/date=<yyyy>-<mm>-<dd>/<timestamp>-<datatype>-<node>-<experiment>-index1.jsonl
//	|------GCSConfig.IndexDir-----|                                   |------GCSConfig.BaseID------|
func New(bucket, gcsDataDir, gcsIndexDir, gcsBaseID, datatype, dateSubdir string) *JSONLBundle {
	nowUTC := time.Now().UTC()
	return &JSONLBundle{
		Lines:      []string{},
		BadFiles:   []string{},
		Index:      []api.IndexV1{},
		Timestamp:  nowUTC.Format("2006/01/02T150405.000000Z"),
		Datatype:   datatype,
		DateSubdir: dateSubdir,
		BundleDir:  dirName(gcsDataDir, nowUTC),
		BundleName: objectName(nowUTC, gcsBaseID, "data"),
		IndexDir:   dirName(gcsIndexDir, nowUTC),
		IndexName:  objectName(nowUTC, gcsBaseID, "index1"),
		Size:       0,
		bucket:     bucket,
	}
}

// Description returns a string describing the bundle for log messages.
func (jb *JSONLBundle) Description() string {
	return fmt.Sprintf("bundle <%v %v %v>", jb.Timestamp, jb.Datatype, jb.DateSubdir)
}

// HasFile returns true or false depending on whether the bundle includes
// (or, for bad files, knows about) the given file or not.
func (jb *JSONLBundle) HasFile(fullPath string) bool {
	for _, index := range jb.Index {
		if index.Filename == fullPath {
			return true
		}
	}
	for _, badFile := range jb.BadFiles {
		if badFile == fullPath {
			return true
		}
	}
	return false
}

// AddFile adds the specified measurement data file in JSON format to
// the bundle by embedding it in the Raw field of M-Lab's standard columns.
// It also adds an index describing the file to the bundle's index.
func (jb *JSONLBundle) AddFile(fullPath, version, gitCommit string) error {
	contents, err := readJSONFile(fullPath)
	if err != nil {
		jb.BadFiles = append(jb.BadFiles, fullPath)
		return err
	}
	stdCols := api.StandardColumnsV0{
		Date: strings.ReplaceAll(jb.DateSubdir, "/", "-"),
		Archiver: api.ArchiverV0{
			Version:    version,
			GitCommit:  gitCommit,
			ArchiveURL: fmt.Sprintf("gs://%s/%s/%s", jb.bucket, jb.BundleDir, jb.BundleName),
			Filename:   fullPath,
		},
		Raw: "", // placeholder for measurement data
	}
	stdColsBytes, err := json.Marshal(stdCols)
	if err != nil {
		return fmt.Errorf("%v: %w", ErrMarshalStdCols, err)
	}
	// Replace the placeholder Raw with the actual measurement data.
	line := strings.Replace(string(stdColsBytes), `"Raw":""`, `"Raw":`+contents, 1)
	jb.Lines = append(jb.Lines, line)

	// Add the file to the bundle's index.
	jb.Index = append(jb.Index, api.IndexV1{
		Filename:  fullPath,
		Size:      len(line),
		TimeAdded: time.Now().UTC().Format("2006/01/02T150405.000000Z"),
	})

	// Update bundle's size.
	jb.Size += uint(len(line))
	verbose("added %v to %v", fullPath, jb.Description())
	return nil
}

// IndexFilenames returns all filenames in the index.
func (jb *JSONLBundle) IndexFilenames() []string {
	indexFilenames := make([]string, len(jb.Index))
	for i, index := range jb.Index {
		indexFilenames[i] = index.Filename
	}
	return indexFilenames
}

// MarshalIndex marshals the index.
func (jb *JSONLBundle) MarshalIndex() ([]byte, error) {
	marshaledIndex := make([]string, len(jb.Index))
	for i, index := range jb.Index {
		indexBytes, err := json.Marshal(index)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", ErrMarshalIndex, err)
		}
		marshaledIndex[i] = string(indexBytes)
	}
	return []byte(strings.Join(marshaledIndex, "\n")), nil
}

// RemoveLocalFiles removes files on the local filesystem that were
// successfully uploaded via this bundle.  If a file cannot be removed,
// an error message is logged but no further action is taken.
func (jb *JSONLBundle) RemoveLocalFiles() {
	for _, index := range jb.Index {
		verbose("removing uploaded data file %v", index.Filename)
		if err := os.Remove(index.Filename); err != nil {
			log.Printf("ERROR: failed to remove uploaded data file: %v\n", err)
		}
	}
	for _, fullPath := range jb.BadFiles {
		verbose("removing bad data file %v", fullPath)
		if err := os.Remove(fullPath); err != nil {
			log.Printf("ERROR: failed to remove bad data file: %v\n", err)
		}
	}
}

// readJSONFile reads the specified file and returns its contents if it
// is valid JSON.
func readJSONFile(fullPath string) (string, error) {
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		return "", fmt.Errorf("%v: %w", err, ErrReadFile)
	}
	if len(bytes) == 0 {
		return "", fmt.Errorf("%v: %w", fullPath, ErrEmptyFile)
	}
	if !json.Valid(bytes) {
		return "", fmt.Errorf("%v: %w", fullPath, ErrInvalidJSON)
	}
	contents := strings.TrimSuffix(string(bytes), "\n")
	if strings.Count(contents, "\n") != 0 {
		return "", fmt.Errorf("%v: %w", fullPath, ErrNotOneLine)
	}
	return contents, nil
}

func objectName(t time.Time, gcsBaseID, bundleType string) string {
	return fmt.Sprintf("%s-%s-%s.jsonl.gz", t.Format("20060102T150405.000000Z"), gcsBaseID, bundleType)
}

func dirName(gcsDir string, t time.Time) string {
	return fmt.Sprintf("%s/date=%s", gcsDir, t.Format("2006-01-02"))
}
