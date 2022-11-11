// Package watchdir watches a directory and sends notifications to its
// client when it notices a new file.
package watchdir //nolint:testpackage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rjeczalik/notify"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		name            string
		watchDir        string
		watchExtensions []string
		watchEvents     []notify.Event
		missedAge       time.Duration
		missedInterval  time.Duration
		wantErr         error
	}{
		{
			name:            "nil watchEvents",
			watchDir:        "/some/path",
			watchExtensions: []string{".json"},
			watchEvents:     nil,
			missedAge:       3 * time.Hour,
			missedInterval:  30 * time.Minute,
			wantErr:         nil,
		},
		{
			name:            "specific watchEvents",
			watchDir:        "/some/path",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       3 * time.Hour,
			missedInterval:  30 * time.Minute,
			wantErr:         nil,
		},
	}
	for i, test := range tests {
		t.Logf(">>> test %02d %s", i, test.name)
		_, err := New(test.watchDir, test.watchExtensions, test.watchEvents, test.missedAge, test.missedInterval)
		if !errors.Is(err, test.wantErr) {
			t.Fatalf("New() = %v, want: %v", err, test.wantErr)
		}
	}
}

func TestWatchAndNotify(t *testing.T) { //nolint:paralleltest,funlen
	defer func() {
		os.RemoveAll("testdata/j.json")
		os.RemoveAll("testdata/t.txt")
	}()
	tests := []struct {
		name            string
		file            string
		missed          bool
		ack             bool
		watchDir        string
		watchExtensions []string
		watchEvents     []notify.Event
		missedAge       time.Duration
		missedInterval  time.Duration
	}{
		{
			name:            "no file creation",
			file:            "",
			missed:          false,
			ack:             false,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
		{
			name:            "new t.txt",
			file:            "t.txt",
			missed:          false,
			ack:             false,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
		{
			name:            "new j.json, acknowledge",
			file:            "j.json",
			missed:          false,
			ack:             true,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
		{
			name:            "new j.json, do not acknowledge",
			file:            "j.json",
			missed:          false,
			ack:             false,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
		{
			name:            "unrecognized event",
			file:            "j.json",
			missed:          false,
			ack:             false,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
		{
			name:            "missed j.json",
			file:            "j.json",
			missed:          true,
			ack:             true,
			watchDir:        "testdata",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       1 * time.Second,
			missedInterval:  1 * time.Second,
		},
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() = %v, want nil", err)
	}
	for i, test := range tests {
		t.Logf(">>> test %02d %s", i, test.name)
		testFile := prepareFile(t, cwd, test.file, test.watchDir, test.missed, test.missedAge)
		wd, err := New(filepath.Join(cwd, test.watchDir), test.watchExtensions, test.watchEvents, test.missedAge, test.missedInterval)
		if err != nil {
			t.Fatalf("New() = %v, want: nil", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_ = wd.WatchAndNotify(ctx)
		}()
		// Give WatchAndNotify() goroutine time to start.
		<-time.After(200 * time.Millisecond)
		fileActivity(t, wd, testFile, test.ack)

		// Cancel context to end WatchAndNotify().
		t.Logf(">>> waiting 2 seconds before canceling ctx")
		<-time.After(2 * time.Second)
		cancel()
		// Wait a little before starting the next iteration.
		time.Sleep(1 * time.Second)
	}
}

func prepareFile(t *testing.T, cwd, file, watchDir string, missed bool, missedAge time.Duration) string {
	t.Helper()
	if file == "" {
		return ""
	}
	testFile := filepath.Join(cwd, watchDir, file)
	if missed {
		// This is going to be a missed file; create it now
		// and wait until it's considered aged.
		if err := os.WriteFile(testFile, []byte{}, 0o666); err != nil {
			t.Fatalf("os.WriteFile() = %v, want: nil", err)
		}
		<-time.After(missedAge)
	} else {
		// This is going to be a new file; make sure it doesn't
		// exist.
		os.RemoveAll(testFile)
	}
	return testFile
}

func fileActivity(t *testing.T, wd *WatchDir, testFile string, ack bool) {
	t.Helper()
	if testFile != "" {
		if err := os.WriteFile(testFile, []byte{}, 0o666); err != nil {
			t.Fatalf("os.WriteFile() = %v, want: nil", err)
		}
	}
	if !ack {
		return
	}
	// We should receive an event either for the file that we created
	// or for a missed file that was previously created.
	watchEvent, chOpen := <-wd.WatchChan()
	if !chOpen {
		t.Fatal("watch channel closed")
	}
	if testFile != "" {
		if watchEvent.Path != testFile {
			t.Fatalf("wd.WatchChan() = %v, want: %v", watchEvent.Path, testFile)
		}
		// Acknowledge receipt of the event.
		wd.WatchAckChan() <- []string{testFile}
	}
}
