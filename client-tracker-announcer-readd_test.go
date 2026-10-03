package torrent

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/metainfo"
)

// An HTTP tracker that keeps the events it is sent in order and answers each announce with one
// peer. The first announce with an event in hold is answered once that channel is closed.
type eventTracker struct {
	interval string
	hold     map[string]chan struct{}
	mu       sync.Mutex
	events   []string
}

func (me *eventTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	event := r.URL.Query().Get("event")
	me.mu.Lock()
	me.events = append(me.events, event)
	release := me.hold[event]
	delete(me.hold, event)
	me.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}
	w.Write([]byte("d8:intervali" + me.interval + "e5:peers6:\xc0\x00\x02\x01\x1a\xe1e"))
}

func (me *eventTracker) seen() []string {
	me.mu.Lock()
	defer me.mu.Unlock()
	return slices.Clone(me.events)
}

func eventually(cond func() bool) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func newEventTrackerClient(t *testing.T, tr *eventTracker) (cl *Client, add func() *Torrent) {
	s := httptest.NewServer(tr)
	t.Cleanup(s.Close)
	cfg := TestingConfig(t)
	cfg.DisableTrackers = false
	// Keeps the tracker's peer known, rather than dialled and forgotten.
	cfg.DialForPeerConns = false
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	t.Cleanup(func() { cl.Close() })
	return cl, func() *Torrent {
		to, _ := cl.AddTorrentInfoHash(metainfo.Hash{1})
		to.AddTrackers([][]string{{s.URL + "/announce"}})
		return to
	}
}

func TestTorrentAddedAgainBeforeItsStoppedAnnouncesAndGetsPeers(t *testing.T) {
	tr := &eventTracker{interval: "3600"}
	_, add := newEventTrackerClient(t, tr)
	first := add()
	// The answer, which brings a peer, and not only the request.
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return first.Stats().PendingPeers == 1 })))
	first.Drop()
	to := add()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return len(tr.seen()) == 2 })),
		qt.Commentf("%q", tr.seen()))
	qt.Check(t, qt.DeepEquals(tr.seen(), []string{"started", "started"}))
	qt.Check(t, qt.IsTrue(eventually(func() bool { return to.Stats().PendingPeers == 1 })))
}

func TestTorrentAddedAgainWhileItsStoppedIsOutStartsOver(t *testing.T) {
	release := make(chan struct{})
	tr := &eventTracker{interval: "1", hold: map[string]chan struct{}{"stopped": release}}
	_, add := newEventTrackerClient(t, tr)
	first := add()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return first.Stats().PendingPeers == 1 })))
	first.Drop()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return slices.Contains(tr.seen(), "stopped") })))
	add()
	close(release)
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return len(tr.seen()) >= 3 })))
	qt.Check(t, qt.DeepEquals(tr.seen()[:3], []string{"started", "stopped", "started"}))
}

func TestTorrentAddedAgainWhileItsAnnounceIsOutGetsThePeers(t *testing.T) {
	release := make(chan struct{})
	tr := &eventTracker{interval: "3600", hold: map[string]chan struct{}{"started": release}}
	_, add := newEventTrackerClient(t, tr)
	first := add()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return len(tr.seen()) == 1 })))
	first.Drop()
	to := add()
	close(release)
	qt.Check(t, qt.IsTrue(eventually(func() bool { return to.Stats().PendingPeers == 1 })))
}
