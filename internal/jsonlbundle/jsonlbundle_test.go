package jsonlbundle //nolint:testpackage

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/jostler/internal/testhelper"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) {
	t.Parallel()
	tests := []struct {
		gcsBucket  string
		gcsDataDir string
		gcsBaseID  string
		datatype   string
		dateSubdir string
	}{
		{
			gcsBucket:  "some-bucket",
			gcsDataDir: "some/path/in/gcs",
			gcsBaseID:  "some-string",
			datatype:   "some-datatype",
			dateSubdir: "2022/11/14",
		},
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d%s", testhelper.ANSIPurple, i, testhelper.ANSIEnd)
		gotjb := New(test.gcsBucket, test.gcsDataDir, test.gcsBaseID, test.datatype, test.dateSubdir)
		timestamp, err := time.Parse("2006/01/02T150405.000000Z", gotjb.Timestamp)
		if err != nil {
			t.Fatalf("time.Parse() = %v", err)
		}
		wantjb := newJb(test.gcsBucket, test.gcsDataDir, test.gcsBaseID, test.datatype, test.dateSubdir, timestamp)
		if !reflect.DeepEqual(gotjb, wantjb) {
			t.Fatalf("New() = %+v, want %+v", gotjb, wantjb)
		}
	}
}

func TestDescription(t *testing.T) {
	t.Parallel()
	nowUTC := time.Now().UTC()
	jb := newTestJb(nowUTC)
	wantDescription := fmt.Sprintf("bundle <%v %v %v>", nowUTC.Format("2006/01/02T150405.000000Z"), jb.Datatype, jb.DateSubdir)
	if jb.Description() != wantDescription {
		t.Fatalf("jb.Description() = %v, want %v", jb.Description(), wantDescription)
	}
}

func TestHasFile(t *testing.T) {
	t.Parallel()
	jb := newTestJb(time.Now().UTC())
	jb.FullPaths = []string{"file-1", "file-2", "file-3"}
	if !jb.HasFile("file-2") {
		t.Fatalf("jb.HasFile(file-2) = %v, want true", jb.HasFile("file-2"))
	}
	if jb.HasFile("file-4") {
		t.Fatalf("jb.HasFile(file-4) = %v, want false", jb.HasFile("file-4"))
	}
}

func TestAddFile(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		file    string
		wantErr error
	}{
		{
			file:    "testdata/non-existent.json",
			wantErr: ErrReadFile,
		},
		{
			file:    "testdata/foo1-empty.json",
			wantErr: ErrEmptyFile,
		},
		{
			file:    "testdata/foo1-invalid.json",
			wantErr: ErrInvalidJSON,
		},
		{
			file:    "testdata/foo1-multi-line.json",
			wantErr: ErrNotOneLine,
		},
		{
			file:    "testdata/foo1-valid.json",
			wantErr: nil,
		},
	}
	jb := newTestJb(time.Now().UTC())
	badFiles := 0
	for i, test := range tests {
		t.Logf("%s>>> test %02d %v %s", testhelper.ANSIPurple, i, test.file, testhelper.ANSIEnd)
		gotErr := jb.AddFile(test.file, "v0.1.2", "cafebabe")
		if gotErr == nil && test.wantErr == nil {
			continue
		}
		if (gotErr != nil && test.wantErr == nil) ||
			(gotErr == nil && test.wantErr != nil) ||
			!errors.Is(gotErr, test.wantErr) {
			t.Fatalf("jb.AddFile() = %v, want %v", gotErr, test.wantErr)
		}
		if test.wantErr != nil {
			badFiles++
		}
		if len(jb.BadFiles) != badFiles {
			t.Fatalf("len(jb.BadFiles) = %v, want %v", len(jb.BadFiles), badFiles)
		}
		if len(jb.FullPaths) != i+1-badFiles {
			t.Fatalf("len(jb.FullPaths) = %v, want %v", len(jb.FullPaths), i+1-badFiles)
		}
	}
}

func TestRemoveLocalFiles(t *testing.T) { //nolint:paralleltest
	jb := newTestJb(time.Now().UTC())
	fullPaths := []string{"testdata/fullpath1.json", "testdata/fullpath2.json"}
	badFiles := []string{"testdata/badfile1.json", "testdata/badfile2.json"}
	for _, file := range append(fullPaths, badFiles...) {
		if err := os.WriteFile(file, []byte("some-content"), 0o666); err != nil {
			t.Fatalf("os.WriteFile() = %v, want nil", err)
		}
	}
	jb.FullPaths = fullPaths
	// Add a non-existent file to force a remove error.
	jb.FullPaths = append(jb.FullPaths, "testdata/non-existent-file")
	jb.BadFiles = badFiles
	jb.RemoveLocalFiles()
}

func newTestJb(timestamp time.Time) *JSONLBundle {
	gcsBucket := "some-bucket"
	gcsDataDir := "some/path/in/gcs"
	gcsBaseID := "some-string"
	datatype := "some-datatype"
	dateSubdir := "2022/11/14"
	return newJb(gcsBucket, gcsDataDir, gcsBaseID, datatype, dateSubdir, timestamp)
}

func newJb(bucket, gcsDataDir, gcsBaseID, datatype, dateSubdir string, timestamp time.Time) *JSONLBundle {
	objName := fmt.Sprintf("%s-%s", timestamp.Format("20060102T150405.000000Z"), gcsBaseID)
	return &JSONLBundle{
		Lines:      []string{},
		Timestamp:  timestamp.Format("2006/01/02T150405.000000Z"),
		Datatype:   datatype,
		DateSubdir: dateSubdir,
		bucket:     bucket,
		ObjDir:     fmt.Sprintf("%s/date=%s", gcsDataDir, timestamp.Format("2006-01-02")), // e.g., ndt/pcap/date=2022-09-14
		ObjName:    objName + ".jsonl",
		IdxName:    objName + ".index",
		FullPaths:  []string{},
		BadFiles:   []string{},
		Size:       0,
	}
}
