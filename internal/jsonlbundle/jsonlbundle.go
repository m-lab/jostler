// Package jsonbundle implements logic to process a single JSONL bundle.
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

var (
	ErrReadFile       = errors.New("failed to read file")
	ErrEmptyFile      = errors.New("empty file")
	ErrInvalidJSON    = errors.New("failed to validate JSON")
	ErrNotOneLine     = errors.New("is not one line")
	ErrMarshalStdCols = errors.New("failed to marshal standard columns")
	ErrMarshalIndex   = errors.New("failed to marshal index")

	// Testing and debugging support.
	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new instance of JSONLBundle.
func New(bucket, gcsDataDir, gcsIndexDir, gcsBaseID, datatype, dateSubdir string) *JSONLBundle {
	nowUTC := time.Now().UTC()
	objName := fmt.Sprintf("%s-%s", nowUTC.Format("20060102T150405.000000Z"), gcsBaseID)
	return &JSONLBundle{
		Lines:      []string{},
		BadFiles:   []string{},
		Index:      []api.IndexV1{},
		Timestamp:  nowUTC.Format("2006/01/02T150405.000000Z"),
		Datatype:   datatype,
		DateSubdir: dateSubdir,
		BundleDir:  fmt.Sprintf("%s/date=%s", gcsDataDir, nowUTC.Format("2006-01-02")), // e.g., ndt/pcap/date=2022-09-14
		BundleName: objName + ".jsonl",
		IndexDir:   fmt.Sprintf("%s/date=%s", gcsIndexDir, nowUTC.Format("2006-01-02")), // e.g., ndt/index1/date=2022-09-14
		IndexName:  objName + ".index",
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
		// log.Panicf("failed to marshal standard columns: %v", err) XXX Should we panic?
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
			// log.Panicf("failed to marshal index: %v", err) XXX Should we panic?
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
