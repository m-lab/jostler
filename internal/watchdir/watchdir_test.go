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

	"github.com/m-lab/jostler/internal/testhelper"
	"github.com/rjeczalik/notify"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNew(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		name            string
		eventNames      map[notify.Event]string
		watchDir        string
		watchExtensions []string
		watchEvents     []notify.Event
		missedAge       time.Duration
		missedInterval  time.Duration
		wantErr         error
	}{
		{
			name:            "nil watchEvents",
			eventNames:      nil,
			watchDir:        "/some/path",
			watchExtensions: []string{".json"},
			watchEvents:     nil,
			missedAge:       3 * time.Hour,
			missedInterval:  30 * time.Minute,
			wantErr:         nil,
		},
		{
			name:            "specific watchEvents",
			eventNames:      nil,
			watchDir:        "/some/path",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       3 * time.Hour,
			missedInterval:  30 * time.Minute,
			wantErr:         nil,
		},
		{
			name:            "unrecognized watchEvent",
			eventNames:      map[notify.Event]string{notify.InAccess: "File was accessed"},
			watchDir:        "/some/path",
			watchExtensions: []string{".json"},
			watchEvents:     []notify.Event{notify.InCloseWrite, notify.InMovedTo},
			missedAge:       3 * time.Hour,
			missedInterval:  30 * time.Minute,
			wantErr:         errUnrecognizedEvent,
		},
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d %s%s", testhelper.ANSIPurple, i, test.name, testhelper.ANSIEnd)
		var saveEventNames map[notify.Event]string
		if test.eventNames != nil {
			saveEventNames = eventNames
			eventNames = test.eventNames
		}
		_, err := New(test.watchDir, test.watchExtensions, test.watchEvents, test.missedAge, test.missedInterval)
		if !errors.Is(err, test.wantErr) {
			t.Fatalf("New() = %v, want: %v", err, test.wantErr)
		}
		if test.eventNames != nil {
			eventNames = saveEventNames
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
		eventNames      map[notify.Event]string
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
			eventNames:      nil,
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
			eventNames:      nil,
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
			eventNames:      nil,
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
			eventNames:      nil,
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
			eventNames:      map[notify.Event]string{notify.InAccess: "File was accessed"},
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
			eventNames:      nil,
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
	if testing.Verbose() {
		Verbose(testhelper.VLogf)
		defer Verbose(func(fmt string, args ...interface{}) {})
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() = %v, want nil", err)
	}
	for i, test := range tests {
		t.Logf("%s>>> test %02d %s%s", testhelper.ANSIPurple, i, test.name, testhelper.ANSIEnd)
		testFile := prepareFile(t, cwd, test.file, test.watchDir, test.missed, test.missedAge)
		wd, err := New(filepath.Join(cwd, test.watchDir), test.watchExtensions, test.watchEvents, test.missedAge, test.missedInterval)
		if err != nil {
			t.Fatalf("New() = %v, want: nil", err)
		}

		// Changing eventNames must be done after the call to New().
		var saveEventNames map[notify.Event]string
		if test.eventNames != nil {
			saveEventNames = eventNames
			eventNames = test.eventNames
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
		if test.eventNames != nil {
			eventNames = saveEventNames
		}
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
