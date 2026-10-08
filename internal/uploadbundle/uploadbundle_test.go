package uploadbundle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m-lab/jostler/internal/testhelper"
	"github.com/m-lab/jostler/internal/watchdir"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestVerbose(t *testing.T) {
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) {
	wdClient, err := testhelper.WatchDirNew("/some/path")
	if err != nil {
		t.Fatalf("testhelper.WatchDirNew() = %v, want nil", err)
	}
	tests := []struct {
		name          string
		wdClient      *testhelper.WatchDir
		gcsBucket     string
		gcsDataDir    string
		gcsDataBaseID string
		bundleDataDir string
		wantErr       error
	}{
		{
			name:          "nil wdClient",
			wdClient:      nil,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsDataBaseID: "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsBucket",
			wdClient:      wdClient,
			gcsBucket:     "",
			gcsDataDir:    "some/path/in/gcs",
			gcsDataBaseID: "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsDataDir",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "",
			gcsDataBaseID: "some-string",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string gcsDataBaseID",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsDataBaseID: "",
			bundleDataDir: "/some/path",
			wantErr:       ErrConfig,
		},
		{
			name:          "empty string bundleDataDir",
			wdClient:      wdClient,
			gcsBucket:     "some-bucket",
			gcsDataDir:    "some/path/in/gcs",
			gcsDataBaseID: "some-string",
			bundleDataDir: "",
			wantErr:       ErrConfig,
		},
		{
			name:          "valid args",
			wdClient:      wdClient,
			gcsBucket:     "newclient",
			gcsDataDir:    "some/path/in/gcs",
			gcsDataBaseID: "some-string",
			bundleDataDir: "/some/path",
			wantErr:       nil,
		},
	}
	stClient, err := testhelper.NewClient(context.Background(), "newclient,upload")
	if err != nil {
		t.Fatalf("testhelper.NewClient() = %v, wanted nil", err)
	}
	for i, test := range tests {
		gcsConf := GCSConfig{
			GCSClient: stClient,
			Bucket:    test.gcsBucket,
			DataDir:   test.gcsDataDir,
			BaseID:    test.gcsDataBaseID,
		}
		bundleConf := BundleConfig{
			Datatype: "foo1",
			SpoolDir: test.bundleDataDir,
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

func TestBundleAndUploadCtx(t *testing.T) {
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

func TestBundleAndUploadTooBig(t *testing.T) {
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
	stClient, err := testhelper.NewClient(context.Background(), "newclient,upload")
	if err != nil {
		t.Fatalf("testhelper.NewClient() = %v, wanted nil", err)
	}
	gcsConf := GCSConfig{
		GCSClient: stClient,
		Bucket:    "newclient,upload",
		DataDir:   "testdata/autoload/v1/experiment/datatype",
		IndexDir:  "testdata/autoload/v1/experiment/index1",
		BaseID:    "some-string",
	}
	bundleConf := BundleConfig{
		Datatype: "foo1",
		SpoolDir: "testdata/spool/jostler/foo1",
		SizeMax:  sizeMax,
		AgeMax:   ageMax,
	}
	ubClient, err := New(context.Background(), wdClient, gcsConf, bundleConf)
	if err != nil {
		t.Fatalf("New() = %v, want nil", err)
	}
	return wdClient, ubClient
}

// TestBundleAndUploadOutcomes verifies what happens to the local files and
// to the directory watcher after an upload succeeds and after it fails.
func TestBundleAndUploadOutcomes(t *testing.T) {
	Verbose(testhelper.VLogf)

	// Make retries fast and cheap for the test.
	savedAttempts, savedDelay := uploadAttempts, uploadRetryDelay
	uploadAttempts, uploadRetryDelay = 2, 10*time.Millisecond
	t.Cleanup(func() { uploadAttempts, uploadRetryDelay = savedAttempts, savedDelay })

	validFile := "testdata/spool/jostler/foo1/2022/11/09/valid.json"
	invalidFile := "testdata/spool/jostler/foo1/2022/11/09/invalid.json"

	tests := []struct {
		name         string
		bucket       string
		wantOnDisk   bool    // is valid.json still on disk afterwards?
		wantErrors   float64 // expected increase in jostler_upload_errors_total
		wantDeferred float64 // expected increase in jostler_bundles_deferred_total
	}{
		{
			name:         "upload succeeds: files removed and acknowledged",
			bucket:       "newclient,upload",
			wantOnDisk:   false,
			wantErrors:   0,
			wantDeferred: 0,
		},
		{
			name:         "upload fails: files kept on disk but still acknowledged",
			bucket:       "newclient,upload,failupload",
			wantOnDisk:   true,
			wantErrors:   2, // uploadAttempts
			wantDeferred: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setupDataDir(t)
			errorsBefore := testutil.ToFloat64(jostlerUploadErrors.WithLabelValues("foo1"))
			deferredBefore := testutil.ToFloat64(jostlerBundlesDeferred.WithLabelValues("foo1"))

			wdClient, err := testhelper.WatchDirNew("/some/path")
			if err != nil {
				t.Fatalf("testhelper.WatchDirNew() = %v, want nil", err)
			}
			wdClient.WatchChan() <- watchdir.WatchEvent{Path: validFile}
			wdClient.WatchChan() <- watchdir.WatchEvent{Path: invalidFile}
			stClient, err := testhelper.NewClient(context.Background(), test.bucket)
			if err != nil {
				t.Fatalf("testhelper.NewClient() = %v, wanted nil", err)
			}
			gcsConf := GCSConfig{
				GCSClient: stClient,
				Bucket:    test.bucket,
				DataDir:   "testdata/autoload/v1/experiment/datatype",
				IndexDir:  "testdata/autoload/v1/experiment/index1",
				BaseID:    "some-string",
			}
			bundleConf := BundleConfig{
				Datatype: "foo1",
				SpoolDir: "testdata/spool/jostler/foo1",
				SizeMax:  20 * 1024 * 1024,
				AgeMax:   200 * time.Millisecond, // upload is triggered by age
			}
			ubClient, err := New(context.Background(), wdClient, gcsConf, bundleConf)
			if err != nil {
				t.Fatalf("New() = %v, want nil", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				ubClient.BundleAndUpload(ctx)
				close(done)
			}()

			// The upload (and any retries) completes in the background;
			// the acknowledgement is the signal that it is finished.
			var acked []string
			select {
			case acked = <-wdClient.Acks():
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for the bundle's files to be acknowledged")
			}
			cancel()
			<-done

			wantAcked := map[string]bool{validFile: true, invalidFile: true}
			if len(acked) != len(wantAcked) {
				t.Errorf("acknowledged %v, want %v", acked, wantAcked)
			}
			for _, f := range acked {
				if !wantAcked[f] {
					t.Errorf("unexpected acknowledged file %v", f)
				}
			}
			_, statErr := os.Stat(validFile)
			if gotOnDisk := statErr == nil; gotOnDisk != test.wantOnDisk {
				t.Errorf("%v on disk = %v, want %v", validFile, gotOnDisk, test.wantOnDisk)
			}
			if got := testutil.ToFloat64(jostlerUploadErrors.WithLabelValues("foo1")) - errorsBefore; got != test.wantErrors {
				t.Errorf("jostler_upload_errors_total increased by %v, want %v", got, test.wantErrors)
			}
			if got := testutil.ToFloat64(jostlerBundlesDeferred.WithLabelValues("foo1")) - deferredBefore; got != test.wantDeferred {
				t.Errorf("jostler_bundles_deferred_total increased by %v, want %v", got, test.wantDeferred)
			}
		})
	}
}
