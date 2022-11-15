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

// JSONLBundle defines a collection of JSON file contents bundled together
// in JSONL format for archiving.
type JSONLBundle struct {
	Lines      []string
	Timestamp  string   // bundle's in-memory creation time that serves as its identifier
	Datatype   string   // bundle's datatype
	DateSubdir string   // date subdirectory of files in this bundle (yyyy/mm/dd)
	bucket     string   // GCS bucket
	ObjDir     string   // GCS directory to upload this bundle to
	ObjName    string   // GCS object name of this bundle
	IdxName    string   // name of the index file for this bundle
	FullPaths  []string // pathnames of individual JSON files
	BadFiles   []string // pathnames of files that could not be read or were not proper JSON
	Size       uint     // size of this bundle
}

var (
	ErrEmptyFile   = errors.New("empty file")
	ErrInvalidJSON = errors.New("failed to validate JSON")
	ErrNotOneLine  = errors.New("is not one line")

	// Testing and debugging support.
	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose provides a convenient way for the caller to enable verbose
// printing and control its format (mostly for debugging).
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new instance of JSONLBundle.
func New(bucket, gcsDataDir, gcsBaseID, datatype, dateSubdir string) *JSONLBundle {
	nowUTC := time.Now().UTC()
	objName := fmt.Sprintf("%s-%s", nowUTC.Format("20060102T150405.000000Z"), gcsBaseID)
	return &JSONLBundle{
		Lines:      []string{},
		Timestamp:  time.Now().UTC().Format("2006/01/02T150405.000000Z"),
		Datatype:   datatype,
		DateSubdir: dateSubdir,
		bucket:     bucket,
		ObjDir:     fmt.Sprintf("%s/%s", gcsDataDir, nowUTC.Format("2006/01/02")), // e.g., ndt/pcap/2022/09/14
		ObjName:    objName + ".jsonl",
		IdxName:    objName + ".index",
		FullPaths:  []string{},
		BadFiles:   []string{},
		Size:       0,
	}
}

// Description returns a string describing the bundle for log messages.
func (jb *JSONLBundle) Description() string {
	return fmt.Sprintf("bundle <%v %v %v>", jb.Timestamp, jb.Datatype, jb.DateSubdir)
}

// HasFile returns true or false depending on whether the bundle includes
// the given file or not.
func (jb *JSONLBundle) HasFile(fullPath string) bool {
	for _, p := range append(jb.FullPaths, jb.BadFiles...) {
		if p == fullPath {
			return true
		}
	}
	return false
}

// AddFile adds the specified file to the bundle.
func (jb *JSONLBundle) AddFile(fullPath string) error {
	contents, err := readJSONFile(fullPath)
	if err != nil {
		jb.BadFiles = append(jb.BadFiles, fullPath)
		return err
	}
	stdCols := api.StandardColumnsV0{
		Date: jb.DateSubdir,
		Archiver: api.ArchiverV0{
			Version:    "jostler@0.1.7",
			GitCommit:  "3ac4528",
			ArchiveURL: fmt.Sprintf("gs://%s/%s/%s", jb.bucket, jb.ObjDir, jb.ObjName),
			Filename:   fullPath,
		},
		Raw: "",
	}
	stdColsBytes, err := json.Marshal(stdCols)
	if err != nil {
		log.Panicf("failed to marshal standard columns: %v", err)
	}
	line := fmt.Sprintf("%s,\"raw\":%s}", strings.TrimSuffix(string(stdColsBytes), "}"), contents)
	jb.Lines = append(jb.Lines, line)
	jb.FullPaths = append(jb.FullPaths, fullPath)
	jb.Size += uint(len(contents))
	verbose("added %v to %v", fullPath, jb.Description())
	return nil
}

// RemoveLocalFiles removes files on the local filesystem that were
// successfully uploaded via this bundle.
func (jb *JSONLBundle) RemoveLocalFiles() {
	for _, fullPath := range append(jb.FullPaths, jb.BadFiles...) {
		verbose("removing %v", fullPath)
		if err := os.Remove(fullPath); err != nil {
			log.Printf("ERROR: failed to remove: %v\n", err)
		}
	}
}

// readJSONFile reads the specified file and returns its contents if it
// is valid JSON.
func readJSONFile(fullPath string) (string, error) {
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
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
