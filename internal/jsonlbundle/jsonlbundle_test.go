package jsonlbundle

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"

	"github.com/m-lab/jostler/api"
	"github.com/m-lab/jostler/internal/testhelper"
)

func TestVerbose(t *testing.T) {
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) {
	t.Parallel()
	tests := []struct {
		gcsBucket   string
		gcsDataDir  string
		gcsIndexDir string
		gcsBaseID   string
		datatype    string
		date        civil.Date
	}{
		{
			gcsBucket:   "some-bucket",
			gcsDataDir:  "some/path/in/gcs",
			gcsIndexDir: "some/path/in/gcs",
			gcsBaseID:   "some-string",
			datatype:    "some-datatype",
			date:        civil.Date{Year: 2022, Month: time.November, Day: 14},
		},
	}
	for i, test := range tests {
		test := test
		t.Logf("%s>>> test %02d%s", testhelper.ANSIPurple, i, testhelper.ANSIEnd)
		gotjb := New(test.gcsBucket, test.gcsDataDir, test.gcsIndexDir, test.gcsBaseID, test.datatype, test.date)
		timestamp, err := time.Parse("2006/01/02T150405.000000Z", gotjb.Timestamp)
		if err != nil {
			t.Fatalf("time.Parse() = %v", err)
		}
		wantjb := newJb(test.gcsBucket, test.gcsDataDir, test.gcsIndexDir, test.gcsBaseID, test.datatype, test.date, timestamp)
		if !reflect.DeepEqual(gotjb, wantjb) {
			t.Fatalf("New() = %+v, want %+v", gotjb, wantjb)
		}
	}
}

func TestDescription(t *testing.T) {
	t.Parallel()
	nowUTC := time.Now().UTC()
	jb := newTestJb(nowUTC)
	wantDescription := fmt.Sprintf("bundle <%v %v %v>", nowUTC.Format("2006/01/02T150405.000000Z"), jb.Datatype, jb.Date)
	if jb.Description() != wantDescription {
		t.Fatalf("jb.Description() = %v, want %v", jb.Description(), wantDescription)
	}
}

func TestHasFile(t *testing.T) {
	t.Parallel()
	jb := newTestJb(time.Now().UTC())
	jb.Index = []api.IndexV1{
		{Filename: "file-1", Size: 0, TimeAdded: ""},
		{Filename: "file-2", Size: 0, TimeAdded: ""},
		{Filename: "file-3", Size: 0, TimeAdded: ""},
	}
	jb.BadFiles = []string{"bad-file-1", "bad-file-2"}
	tests := []struct {
		file   string
		exists bool
	}{
		{"file-2", true},
		{"file-4", false},
		{"bad-file-1", true},
		{"bad-file-3", false},
	}
	for i, test := range tests {
		test := test
		t.Logf("%s>>> test %02d%s", testhelper.ANSIPurple, i, testhelper.ANSIEnd)
		if exists := jb.HasFile(test.file); exists != test.exists {
			t.Fatalf("jb.HasFile(%v) = %v, want %v", test.file, exists, test.exists)
		}
	}
}

func TestAddFile(t *testing.T) {
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
	var wantIndex []string
	for i, test := range tests {
		t.Logf("%s>>> test %02d %v %s", testhelper.ANSIPurple, i, test.file, testhelper.ANSIEnd)
		gotErr := jb.AddFile(test.file, "v0.1.2", "cafebabe")
		if gotErr == nil && test.wantErr == nil {
			wantIndex = append(wantIndex, test.file)
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
		if len(jb.Index) != i+1-badFiles {
			t.Fatalf("len(jb.Index) = %v, want %v", len(jb.Index), i+1-badFiles)
		}
	}
	gotIndex := jb.IndexFilenames()
	if len(gotIndex) != len(wantIndex) {
		t.Fatalf("len(gotIndex) = %v, want %v", len(gotIndex), len(wantIndex))
	}
	if _, err := jb.MarshalIndex(); err != nil {
		t.Fatalf("jb.MarshalIndex() = %v, want nil", err)
	}
}

func TestRemoveLocalFiles(t *testing.T) {
	jb := newTestJb(time.Now().UTC())
	fullPaths := []string{"testdata/fullpath1.json", "testdata/fullpath2.json"}
	badFiles := []string{"testdata/badfile1.json", "testdata/badfile2.json"}
	for _, file := range append(fullPaths, badFiles...) {
		if err := os.WriteFile(file, []byte("some-content"), 0o666); err != nil {
			t.Fatalf("os.WriteFile() = %v, want nil", err)
		}
	}
	for _, fullPath := range fullPaths {
		jb.Index = append(jb.Index, api.IndexV1{Filename: fullPath, Size: 0, TimeAdded: ""})
	}
	// Add a non-existent file to force a remove error.
	jb.Index = append(jb.Index, api.IndexV1{Filename: "testdata/non-existent-file", Size: 0, TimeAdded: ""})
	jb.BadFiles = badFiles
	jb.RemoveLocalFiles()
}

func newTestJb(timestamp time.Time) *JSONLBundle {
	gcsBucket := "some-bucket"
	gcsDataDir := "some/path/in/gcs"
	gcsIndexDir := "some/path/in/gcs"
	gcsBaseID := "some-string"
	datatype := "some-datatype"
	date := civil.Date{Year: 2022, Month: time.November, Day: 14}
	return newJb(gcsBucket, gcsDataDir, gcsIndexDir, gcsBaseID, datatype, date, timestamp)
}

func newJb(bucket, gcsDataDir, gcsIndexDir, gcsBaseID, datatype string, date civil.Date, timestamp time.Time) *JSONLBundle {
	return &JSONLBundle{
		Lines:      []string{},
		BadFiles:   []string{},
		Index:      []api.IndexV1{},
		Timestamp:  timestamp.Format("2006/01/02T150405.000000Z"),
		Datatype:   datatype,
		Date:       date,
		BundleDir:  dirName(gcsDataDir, timestamp),
		BundleName: objectName(timestamp, gcsBaseID, "data"),
		IndexDir:   dirName(gcsIndexDir, timestamp),
		IndexName:  objectName(timestamp, gcsBaseID, "index1"),
		Size:       0,
		bucket:     bucket,
	}
}

func Test_dirName(t *testing.T) {
	dir := "gs://bucket/autoload/v1/experiment/datatype"
	time := time.Date(2023, 03, 30, 4, 4, 0, 0, time.UTC)
	want := dir + "/2023/03/30"
	if got := dirName(dir, time); got != want {
		t.Errorf("dirName() = %v, want %v", got, want)
	}
}
