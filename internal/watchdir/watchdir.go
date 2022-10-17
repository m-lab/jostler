// Package watchdir watches a directory and sends notifications to its
// client when it notices a new file.
package watchdir

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rjeczalik/notify"
)

// WatchEvent is the message that is passed through the watch channel.
type WatchEvent struct {
	Path string // file pathname
}

// WatchDir defines the directory (and possiblty all its subdirectories)
// to watch.
type WatchDir struct {
	watchDir        string              // directory to watch
	watchExtensions map[string]struct{} // filename extensions to watch (empty means everything)
	watchEvents     []notify.Event      // events to watch for
	missedAge       time.Duration       // a file's minimum age before it's considered missed
	missedInterval  time.Duration       // internval for scanning filesystem for missed files
}

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
	eventNames = map[notify.Event]string{
		notify.InAccess:       "File was accessed",
		notify.InModify:       "File was modified",
		notify.InAttrib:       "Metadata changed",
		notify.InCloseWrite:   "Writable file was closed",
		notify.InCloseNowrite: "Unwrittable file closed",
		notify.InOpen:         "File was opened",
		notify.InMovedFrom:    "File was moved from X",
		notify.InMovedTo:      "File was moved to Y",
		notify.InCreate:       "Subfile was created",
		notify.InDelete:       "Subfile was deleted",
		notify.InDeleteSelf:   "Self was deleted",
		notify.InMoveSelf:     "Self was moved",
	}

	errUnrecognizedEvent = errors.New("unrecognized event")

	verbose = func(fmt string, args ...interface{}) {}
)

// Verbose prints verbose messages if initialized by the caller.
func Verbose(v func(string, ...interface{})) {
	verbose = v
}

// New returns a new instance of WatchDir.
func New(watchDir string, watchExtensions []string, watchEvents []notify.Event, missedAge, missedInterval time.Duration) (*WatchDir, error) {
	// Validate events.
	if len(watchEvents) > 0 {
		for _, e := range watchEvents {
			if _, ok := eventNames[e]; !ok {
				return nil, fmt.Errorf("%v: %w", e, errUnrecognizedEvent)
			}
		}
	} else {
		watchEvents = AllWatchEvents
	}
	wd := &WatchDir{
		watchDir:        filepath.Clean(watchDir),
		watchExtensions: make(map[string]struct{}),
		watchEvents:     watchEvents,
		missedAge:       missedAge,
		missedInterval:  missedInterval,
	}
	for _, ext := range watchExtensions {
		wd.watchExtensions[ext] = struct{}{}
	}
	return wd, nil
}

// WatchAndNotify watches a directory (and possibly all its subdirectories)
// for the configured events and sends the pathnames of the events it received
// through the configured channel.
func (wd *WatchDir) WatchAndNotify(ctx context.Context) {
	<-ctx.Done()
	verbose("context canceled; returning")
}
