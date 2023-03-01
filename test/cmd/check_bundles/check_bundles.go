// This tool is a part of e2e helper programs and verifies that:
//
//  1. For every index bundle there is a data bundle and vice versa.
//  2. Every file specified in the index bundle exists in the data
//     bundle and vice versa.
//  3. The order in which files appear in the index and data bundles
//     is the same.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/m-lab/jostler/api"
)

var verbose = flag.Bool("verbose", false, "enable verbose mode")

// StandardColumnsV0 defines version 0 of the standard columns included
// in every line (row) along with the raw data from the measurement service.
//
// We have to define it because Raw has to be defined as string in
// api.StandardColumnsV0 and as any here.
type StandardColumnsV0 struct {
	Archiver api.ArchiverV0 `bigquery:"archiver"` // archiver details
	Raw      any            `bigquery:"raw"`      // measurement data (file contents) in JSON format
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		walkDir(".")
	} else {
		for _, arg := range flag.Args() {
			walkDir(arg)
		}
	}
}

func walkDir(dir string) {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Panicf("failed to access path: %v", err)
		}
		bundleType := ""
		if strings.HasSuffix(path, "-data.jsonl.gz") {
			bundleType = "data" //nolint:goconst
		} else if strings.HasSuffix(path, "-index1.jsonl.gz") {
			bundleType = "index"
		}
		if bundleType != "" {
			checkBundle(path, bundleType)
		}
		return nil
	})
	if err != nil {
		log.Panicf("failed to walk directory %v: %v", dir, err)
	}
}

func checkBundle(thisBundle, bundleType string) { //nolint:funlen,cyclop
	if *verbose {
		fmt.Printf("\nchecking %v bundle %v\n", bundleType, thisBundle) //nolint:forbidigo
	}
	// 1. Verify we can open the bundle.
	thisFi, err := os.Open(thisBundle)
	if err != nil {
		log.Panicf("failed to open %v: %v", thisBundle, err)
	}
	defer thisFi.Close()

	// 2. Verify there is corresponding bundle for this bundle.
	var otherBundle string
	switch bundleType {
	case "data":
		otherBundle = strings.ReplaceAll(thisBundle, "/datatype1/", "/index1/")
		otherBundle = strings.ReplaceAll(otherBundle, "-data.jsonl.gz", "-index1.jsonl.gz")
	case "index":
		otherBundle = strings.ReplaceAll(thisBundle, "/index1/", "/datatype1/")
		otherBundle = strings.ReplaceAll(otherBundle, "-index1.jsonl.gz", "-data.jsonl.gz")
	default:
		log.Panicf("invalid bundle type %v", bundleType)
	}
	otherFi, err := os.Open(otherBundle)
	if err != nil {
		log.Panicf("failed to open %v: %v", otherBundle, err)
	}
	defer otherFi.Close()

	// 3. Verify for each entry in the this bundle, there is a
	// corresponding entry in the other bundle at the same order.
	r, err := gzip.NewReader(thisFi)
	if err != nil {
		log.Panicf("failed to instantiate reader %v: %v", thisBundle, err)
	}
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)
	for i := 0; s.Scan(); i++ {
		t := s.Text()
		var err error
		var filename string
		if bundleType == "data" {
			var stdCols StandardColumnsV0
			err = json.Unmarshal([]byte(t), &stdCols)
			filename = stdCols.Archiver.Filename
		} else {
			var index api.IndexV1
			err = json.Unmarshal([]byte(t), &index)
			filename = index.Filename
		}
		if err != nil || filename == "" {
			log.Panicf("failed to unmarshal %v line %v: %v", bundleType, t, err)
		}
		if ret, err := otherFi.Seek(0, io.SeekStart); ret != 0 || err != nil {
			log.Panicf("failed to rewind bundle %v", otherBundle)
		}
		if !fileInBundle(otherFi, bundleType, filename, i) {
			log.Panicf("failed to find %v in index bundle %v", filename, otherBundle)
		}
	}

	if err := r.Close(); err != nil {
		log.Panicf("failed to close reader %v: %v", thisBundle, err)
	}
}

func fileInBundle(bundleFi *os.File, bundleType, filename string, order int) bool {
	if *verbose {
		fmt.Printf("%s at %d ", filename, order) //nolint:forbidigo
	}
	r, err := gzip.NewReader(bundleFi)
	if err != nil {
		log.Panicf("failed to instantiate reader for index bundle %v: %v", bundleFi.Name(), err)
	}
	defer r.Close()
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)
	for i := 0; s.Scan(); i++ {
		t := s.Text()
		var err error
		var f string
		// Note that we are unmarshaling the "other" bundle.
		if bundleType == "data" {
			var index api.IndexV1
			err = json.Unmarshal([]byte(t), &index)
			f = index.Filename
		} else {
			var stdCols StandardColumnsV0
			err = json.Unmarshal([]byte(t), &stdCols)
			f = stdCols.Archiver.Filename
		}
		if err != nil {
			log.Panicf("failed to unmarshal data line %v: %v", t, err)
		}
		if f == filename {
			if *verbose {
				fmt.Printf("FOUND at %d\n", i) //nolint:forbidigo
			}
			if i != order {
				log.Panicf("mismatch in order (%d != %d)", i, order)
			}
			return true
		}
	}
	fmt.Println("NOT FOUND") //nolint:forbidigo
	return false
}
