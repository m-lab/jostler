package uploadbundle

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

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
