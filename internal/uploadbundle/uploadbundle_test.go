package uploadbundle //nolint:testpackage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m-lab/jostler/internal/testhelper"
	"github.com/m-lab/jostler/internal/watchdir"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) { //nolint:paralleltest
	wdClient, err := testhelper.WatchDirNew("/some/path")
	if err != nil {
		t.Fatalf("testhelper.WatchDirNew() = %v, want nil", err)
	}
	tests := []struct {
		name          string
		wdClient      *testhelper.WatchDir
		gcsBucket     string
		gcsDataDir    string
		gcsBaseID     string
		bundleDataDir string
		wantErr       error
	}{
		{
			name:          "nil wdClient",
			wdClient:      nil,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsBaseID:     "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsBucket",
			wdClient:      wdClient,
			gcsBucket:     "",
			gcsDataDir:    "some/path/in/gcs",
			gcsBaseID:     "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsDataDir",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "",
			gcsBaseID:     "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsBaseID",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsBaseID:     "",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string bundleDataDir",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsBaseID:     "some-string",
			bundleDataDir: "",
			wantErr:       ErrConfig,
		},
		{
			name:          "valid args",
			wdClient:      wdClient,
			gcsBucket:     "newclient",
			gcsDataDir:    "some/path/in/gcs",
			gcsBaseID:     "some-string",
			bundleDataDir: "/some/path",
			wantErr:       nil,
		},
	}
	saveGCSClient := GCSClient
	GCSClient = testhelper.DiskNewClient
	defer func() {
		GCSClient = saveGCSClient
	}()
	for i, test := range tests {
		gcsConf := GCSConfig{
			Bucket:  test.gcsBucket,
			DataDir: test.gcsDataDir,
			BaseID:  test.gcsBaseID,
		}
		bundleConf := BundleConfig{
			Datatype: "foo1",
			DataDir:  test.bundleDataDir,
			SizeMax:  20 * 1024 * 1024,
			AgeMax:   1 * time.Hour,
		}
		var s string
		if test.wantErr == nil {
			s = "should succeed"
		} else {
			s = "should fail"
		}
		t.Logf("%s>>> test %02d: %s: %v%s", testhelper.ANSIPurple, i, s, test.name, testhelper.ANSIEnd)
		_, gotErr := New(context.Background(), test.wdClient, gcsConf, bundleConf)
		if gotErr == nil && test.wantErr == nil {
			continue
		}
		if (gotErr != nil && test.wantErr == nil) ||
			(gotErr == nil && test.wantErr != nil) ||
			!errors.Is(gotErr, test.wantErr) {
			t.Fatalf("New() = %v, want %v", gotErr, test.wantErr)
		}
	}
}

func TestBundleAndUploadCtx(t *testing.T) { //nolint:paralleltest
	saveGCSClient := GCSClient
	GCSClient = testhelper.DiskNewClient
	defer func() {
		GCSClient = saveGCSClient
	}()
	Verbose(testhelper.VLogf)

	// BundleAndUpload() returns when its context is canceled.
	setupDataDir(t)
	sizeMax := uint(100)
	ageMax := 2 * time.Second
	_, ubClient := setupClients(t, sizeMax, ageMax)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(ageMax + 2*time.Second)
		t.Logf(">>> canceling context")
		cancel()
	}()
	if err := ubClient.BundleAndUpload(ctx); err != nil {
		t.Fatalf("New() = %v, want nil", err)
	}
}

func TestBundleAndUploadTooBig(t *testing.T) { //nolint:paralleltest
	saveGCSClient := GCSClient
	GCSClient = testhelper.DiskNewClient
	defer func() {
		GCSClient = saveGCSClient
	}()
	Verbose(testhelper.VLogf)

	// Force not enough room in the bundle by setting sizeMax to a
	// ridiculously small value.
	setupDataDir(t)
	sizeMax := uint(1)
	ageMax := 2 * time.Second
	_, ubClient := setupClients(t, sizeMax, ageMax)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(2 * ageMax)
		t.Logf(">>> canceling context")
		cancel()
	}()
	if err := ubClient.BundleAndUpload(ctx); err != nil {
		t.Fatalf("New() = %v, want nil", err)
	}
}

func setupDataDir(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll("testdata/spool/jostler/foo1/2022/11/09", 0o755); err != nil {
		t.Fatalf("os.MkdirAll() = %v, want nil", err)
	}
	for _, file := range []struct {
		name     string
		contents string
	}{
		{name: "nil.json", contents: ""},
		{name: "invalid.json", contents: `{ "Field1": 1 "Field2": 0.1 "NMSVersion": "v1.0.0" }`},
		{name: "valid.json", contents: `{ "Field1": 1, "Field2": 0.1, "NMSVersion": "v1.0.0" }`},
	} {
		f := filepath.Join("testdata/spool/jostler/foo1/2022/11/09/", file.name)
		if err := os.WriteFile(f, []byte(file.contents), 0o644); err != nil {
			t.Fatalf("os.WriteFile() = %v, want nil", err)
		}
	}
	if err := os.MkdirAll("testdata/spool/jostler/foo1/2022/11/09/dir.json", 0o755); err != nil {
		t.Fatalf("os.MkdirAll() = %v, want nil", err)
	}
}

func setupClients(t *testing.T, sizeMax uint, ageMax time.Duration) (*testhelper.WatchDir, *UploadBundle) {
	t.Helper()
	// Create a directory watcher client (local disk).
	wdClient, err := testhelper.WatchDirNew("/some/path")
	if err != nil {
		t.Fatalf("testhelper.WatchDirNew() = %v, want nil", err)
	}
	// Send WatchEvents through WatchChan for the following paths.
	paths := []string{
		"j.json",
		"testdata/spool/jostler/foo1/../j.json",
		"testdata/spool/jostler/foo1",
		"testdata/spool/jostler/foo1/2022/11/09/j,json",
		"testdata/spool/jostler/foo1/2022/11/09/j..json",
		"testdata/spool/jostler/foo1/2022/11/9/j.json",
		"testdata/spool/jostler/foo1/2022/11/09/.j.json",
		"testdata/spool/jostler/foo1/2022/11/09/non-existent.json",
		"testdata/spool/jostler/foo1/2022/11/09/dir.json",
		"testdata/spool/jostler/foo1/2022/11/09/nil.json",
		"testdata/spool/jostler/foo1/2022/11/09/invalid.json",
		"testdata/spool/jostler/foo1/2022/11/09/valid.json",
	}
	for _, path := range paths {
		wdClient.WatchChan() <- watchdir.WatchEvent{Path: path, Missed: false}
	}

	// Create a bundler and uploader client.
	gcsConf := GCSConfig{
		Bucket:  "newclient,upload",
		DataDir: "testdata/autoload/v0",
		BaseID:  "some-string",
	}
	bundleConf := BundleConfig{
		Datatype: "foo1",
		DataDir:  "testdata/spool/jostler/foo1",
		SizeMax:  sizeMax,
		AgeMax:   ageMax,
	}
	ubClient, err := New(context.Background(), wdClient, gcsConf, bundleConf)
	if err != nil {
		t.Fatalf("New() = %v, want nil", err)
	}
	return wdClient, ubClient
}
