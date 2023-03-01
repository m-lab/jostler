// Package watchdir watches a directory and sends notifications to its
// client when it notices a new file.
package watchdir

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rjeczalik/notify"
)

// WatchEvent is the message that is passed through the watch channel.
type WatchEvent struct {
	Path   string // file pathname
	Missed bool   // true if file was missed
}

// WatchDir defines the directory (and possibly all its subdirectories)
// to watch.
type WatchDir struct {
	watchDir          string              // directory to watch
	watchExtensions   map[string]struct{} // filename extensions to watch (empty means everything)
	watchEvents       []notify.Event      // events to watch for
	watchChan         chan WatchEvent     // channel to send watch events through
	watchAckChan      chan []string       // channel for client to acknowledge events received
	missedAge         time.Duration       // a file's minimum age before it's considered missed
	missedInterval    time.Duration       // internval for scanning filesystem for missed files
	notifiedFiles     map[string]struct{} // files for which notification was sent
	notifiedFilesLock sync.Mutex          // lock for notifiedFiles
}

const (
	// The values of these constants should be big enough to allow
	// for a flurry of file creations.
	watchChanSize     = 10000
	notifiedFilesSize = 10000
	notifyChanSize    = 10000
)

var (
	// AllWatchEvents is the list of all possible events to watch for.
	AllWatchEvents = []notify.Event{
		notify.InAccess,
		notify.InModify,
		notify.InAttrib,
		notify.InCloseWrite,
		notify.InCloseNowrite,
		notify.InOpen,
		notify.InMovedFrom,
		notify.InMovedTo,
		notify.InCreate,
		notify.InDelete,
		notify.InDeleteSelf,
		notify.InMoveSelf,
	}

	errUnrecognizedEvent = errors.New("unrecognized event")
	errNotifyWatch       = errors.New("failed to start notify.Watch")

	// Testing and debugging support.
	vFunc     = func(fmt string, args ...interface{}) {}
	vFuncLock sync.Mutex
)

// Verbose prints verbose messages if initialized by the caller.
func Verbose(v func(string, ...interface{})) {
	vFuncLock.Lock()
	vFunc = v
	vFuncLock.Unlock()
}

func verbose(fmt string, args ...interface{}) {
	vFuncLock.Lock()
	vFunc(fmt, args...)
	vFuncLock.Unlock()
}

// New returns a new instance of WatchDir.
func New(watchDir string, watchExtensions []string, watchEvents []notify.Event, missedAge, missedInterval time.Duration) (*WatchDir, error) {
	// If watchEvents is empty, it means all watch events; otherwise,
	// it's a list of specific watch events and we should validate it.
	if len(watchEvents) == 0 {
		watchEvents = AllWatchEvents
	} else {
		if err := validateWatchEvents(watchEvents); err != nil {
			return nil, err
		}
	}
	wd := &WatchDir{
		watchDir:          filepath.Clean(watchDir),
		watchExtensions:   make(map[string]struct{}),
		watchEvents:       watchEvents,
		watchChan:         make(chan WatchEvent, watchChanSize),
		watchAckChan:      make(chan []string, watchChanSize),
		missedAge:         missedAge,
		missedInterval:    missedInterval,
		notifiedFiles:     make(map[string]struct{}, notifiedFilesSize),
		notifiedFilesLock: sync.Mutex{},
	}
	for _, ext := range watchExtensions {
		wd.watchExtensions[ext] = struct{}{}
	}
	return wd, nil
}

// WatchChan returns the channel through which watch events (paths)
// are sent to the client.
func (wd *WatchDir) WatchChan() chan WatchEvent {
	return wd.watchChan
}

// WatchAckChan returns the channel through which client acknowledges
// the watch events it has received and processed, so watchdir can remove
// them from its notifiedFiles map.
func (wd *WatchDir) WatchAckChan() chan<- []string {
	return wd.watchAckChan
}

// WatchAndNotify watches a directory (and possibly all its subdirectories)
// for the configured events and sends the pathnames of the events it received
// through the configured channel.
func (wd *WatchDir) WatchAndNotify(ctx context.Context) error {
	go wd.findMissedAndNotify(ctx)

	verbose("watching directory %v and notifying", wd.watchDir)
	eiChan := make(chan notify.EventInfo, notifyChanSize)
	if err := notify.Watch(wd.watchDir+"/...", eiChan, wd.watchEvents...); err != nil {
		return fmt.Errorf("%v: %w", errNotifyWatch, err)
	}
	defer notify.Stop(eiChan)
	done := false
	for !done {
		select {
		case <-ctx.Done():
			verbose("'watch and notify' context canceled for %v", wd.watchDir)
			done = true
		case ei, chOpen := <-eiChan:
			if !chOpen {
				verbose("event info channel closed")
				done = true
				break
			}
			if err := validateWatchEvents([]notify.Event{ei.Event()}); err != nil {
				log.Printf("WARNING: ignoring unrecognized event %v for %v\n", ei, ei.Path())
				continue
			}
			if !wd.validPath(ei.Path(), nil) {
				verbose("ignoring %v", ei.Path())
				continue
			}
			wd.checkAndNotify(WatchEvent{Path: ei.Path(), Missed: false})
		case fullPaths, chOpen := <-wd.watchAckChan:
			if !chOpen {
				verbose("watch acknowledgement channel closed")
				done = true
				break
			}
			wd.ackNotifications(fullPaths)
		}
	}
	return nil
}

// validateWatchEvents validates that all watch events in the specified
// list are valid.
func validateWatchEvents(watchEvents []notify.Event) error {
	for _, we := range watchEvents {
		found := false
		for i := range AllWatchEvents {
			if we == AllWatchEvents[i] {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%v: %w", we, errUnrecognizedEvent)
		}
	}
	return nil
}

// ackNotifications gets a list of files that the client acknowledges
// was notified about.  These files should be removed from notifiedFiles
// map so that the map wouldn't grow indefinitely.
func (wd *WatchDir) ackNotifications(fullPaths []string) {
	wd.notifiedFilesLock.Lock()
	defer wd.notifiedFilesLock.Unlock()
	for _, fullPath := range fullPaths {
		// Do a sanity check first because delete() does
		// not care if the key is not in the map.
		if _, ok := wd.notifiedFiles[fullPath]; !ok {
			log.Panicf("%v not in notifiedFiles", fullPath)
		}
		delete(wd.notifiedFiles, fullPath)
	}
}

// findMissedAndNotify finds missed files in a directory and all its
// subdirectories that may have been missed by WatchAndNotify() and sends
// the missed pathnames through the configured channel.
func (wd *WatchDir) findMissedAndNotify(ctx context.Context) {
	verbose("scanning %v every %v to find missed files", wd.watchDir, wd.missedInterval)
	for {
		select {
		case <-ctx.Done():
			verbose("'find missed and notify' context canceled for %v", wd.watchDir)
			return
		case <-time.After(wd.missedInterval):
			verbose("'scanning %v", wd.watchDir)
		}

		lastMod := time.Now().Add(-wd.missedAge)
		err := filepath.WalkDir(wd.watchDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return fmt.Errorf("failed to access path: %w", err)
			}
			var fi os.FileInfo
			fi, err = d.Info()
			if err != nil {
				return fmt.Errorf("failed to get file info: %w", err)
			}
			if wd.validPath(path, fi) && lastMod.After(fi.ModTime()) {
				wd.checkAndNotify(WatchEvent{Path: path, Missed: true})
			}
			return nil
		})
		if err != nil {
			// There is a very small chance that while walking
			// the directory to look for possibly missed files,
			// we visit a file that was uploaded and has been
			// removed.
			log.Printf("WARNING: failed to walk directory %v: %v\n", wd.watchDir, err)
		}
	}
}

// checkAndNotify checks if this file is already in the notifiedFiles map.
// If it is, there's nothing to do.  Otherwise, add to the notifiedFiles
// map and send notificatio.
func (wd *WatchDir) checkAndNotify(we WatchEvent) {
	wd.notifiedFilesLock.Lock()
	if _, ok := wd.notifiedFiles[we.Path]; ok {
		wd.notifiedFilesLock.Unlock()
		verbose("notification previously sent for %v", we)
		return
	}
	wd.notifiedFiles[we.Path] = struct{}{}
	wd.notifiedFilesLock.Unlock()
	wd.watchChan <- we
	verbose("notification sent for %v", we)
}

// validPath returns true if the given path has a valid extension and
// is a regular file.
func (wd *WatchDir) validPath(path string, fi os.FileInfo) bool {
	if len(wd.watchExtensions) > 0 {
		if _, ok := wd.watchExtensions[filepath.Ext(path)]; !ok {
			return false
		}
	}
	if fi == nil {
		var err error
		fi, err = os.Stat(path)
		if err != nil {
			log.Printf("WARNING: failed to stat: %v\n", err)
			return false
		}
	}
	return fi.Mode().IsRegular()
}
