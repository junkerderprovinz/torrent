package torrent

import (
	"testing"

	qt "github.com/go-quicktest/qt"
)

func TestReannounceToTrackersAnnouncesBeforeTheIntervalRunsOut(t *testing.T) {
	tr := &eventTracker{interval: "3600"}
	cl, add := newEventTrackerClient(t, tr)
	add()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return len(tr.seen()) == 1 })))

	cl.ReannounceToTrackers()

	qt.Assert(t, qt.IsTrue(eventually(func() bool { return len(tr.seen()) == 2 })),
		qt.Commentf("%q", tr.seen()))
	qt.Check(t, qt.DeepEquals(tr.seen(), []string{"started", ""}))
}
