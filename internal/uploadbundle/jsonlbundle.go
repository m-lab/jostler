package uploadbundle

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/m-lab/jostler/api"
)

// jsonlBundle defines a collection of JSON file contents bundled together
// in JSONL format for archiving.
type jsonlBundle struct {
	lines      []string
	timestamp  string   // bundle's in-memory creation time that serves as its identifier
	datatype   string   // bundle's datatype
	dateSubdir string   // date subdirectory of files in this bundle (yyyy/mm/dd)
	bucket     string   // GCS bucket
	objDir     string   // GCS directory to upload this bundle to
	objName    string   // GCS object name of this bundle
	idxName    string   // name of the index file for this bundle
	fullPaths  []string // pathnames of individual JSON files
	badFiles   []string // pathnames of files that could not be read or were not proper JSON
	size       uint     // size of this bundle
	noRm       bool     // XXX debugging support - delete when done
}

var (
	errEmptyFile   = errors.New("empty file")
	errInvalidJSON = errors.New("failed to validate JSON")
	errNotOneLine  = errors.New("is not one line")
)

func (jb *jsonlBundle) addFile(fullPath string) error {
	contents, err := readJSONFile(fullPath)
	if err != nil {
		jb.badFiles = append(jb.badFiles, fullPath)
		return err
	}
	stdCols := api.StandardColumnsV0{
		Date: jb.dateSubdir,
		Archiver: api.ArchiverV0{
			Version:    "jostler@0.1.7",
			GitCommit:  "3ac4528",
			ArchiveURL: fmt.Sprintf("gs://%s/%s/%s", jb.bucket, jb.objDir, jb.objName),
			Filename:   fullPath,
		},
		Raw: "",
	}
	stdColsBytes, err := json.Marshal(stdCols)
	if err != nil {
		log.Panicf("failed to marshal standard columns: %v", err)
	}
	line := fmt.Sprintf("%s,\"raw\":%s}", strings.TrimSuffix(string(stdColsBytes), "}"), contents)
	jb.lines = append(jb.lines, line)
	jb.fullPaths = append(jb.fullPaths, fullPath)
	jb.size += uint(len(contents))
	verbose("added %v to %v", fullPath, jb.description())
	return nil
}

func readJSONFile(fullPath string) (string, error) {
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}
	if len(bytes) == 0 {
		return "", fmt.Errorf("%v: %w", fullPath, errEmptyFile)
	}
	if !json.Valid(bytes) {
		return "", fmt.Errorf("%v: %w", fullPath, errInvalidJSON)
	}
	contents := strings.TrimSuffix(string(bytes), "\n")
	if strings.Count(contents, "\n") != 0 {
		return "", fmt.Errorf("%v: %w", fullPath, errNotOneLine)
	}
	return contents, nil
}

// description returns a string describing the bundle for log messages.
func (jb *jsonlBundle) description() string {
	return fmt.Sprintf("bundle <%v %v %v>", jb.timestamp, jb.datatype, jb.dateSubdir)
}

// removeFiles removes files on the local filesystem that were
// successfully uploaded via this bundle.
func (jb *jsonlBundle) removeFiles() {
	doPanic := false // XXX debugging support - delete when done
	for _, fullPath := range append(jb.fullPaths, jb.badFiles...) {
		verbose("removing %v", fullPath)
		if err := os.Remove(fullPath); err != nil {
			log.Printf("ERROR: failed to remove: %v\n", err)
			doPanic = true
		}
	}
	if doPanic {
		verbose("len(jb.fullPaths)=%v len(jb.badFiles)=%v", len(jb.fullPaths), len(jb.badFiles))
		panic("removeFiles")
	}
}

// hasFile returns true or false depending on whether the bundle includes
// the given file or not.
func (jb *jsonlBundle) hasFile(fullPath string) bool {
	for _, p := range append(jb.fullPaths, jb.badFiles...) {
		if p == fullPath {
			return true
		}
	}
	return false
}
