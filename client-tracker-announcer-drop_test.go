package torrent

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/metainfo"
)

// An HTTP tracker that asks for an announce every second and counts the announces per infohash.
type countingTracker struct {
	mu    sync.Mutex
	count map[string]int
}

func (me *countingTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	me.mu.Lock()
	me.count[r.URL.Query().Get("info_hash")]++
	me.mu.Unlock()
	w.Write([]byte("d8:intervali1e5:peers0:e"))
}

func (me *countingTracker) announces(ih metainfo.Hash) int {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.count[string(ih[:])]
}

func TestTorrentKeepsAnnouncingAfterAnotherIsDropped(t *testing.T) {
	tr := &countingTracker{count: make(map[string]int)}
	s := httptest.NewServer(tr)
	defer s.Close()
	cfg := TestingConfig(t)
	cfg.DisableTrackers = false
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer cl.Close()
	add := func(ih metainfo.Hash) *Torrent {
		to, _ := cl.AddTorrentInfoHash(ih)
		to.AddTrackers([][]string{{s.URL + "/announce"}})
		return to
	}
	waitFor := func(ih metainfo.Hash, n int, within time.Duration) bool {
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			if tr.announces(ih) >= n {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}
	kept, dropped := metainfo.Hash{1}, metainfo.Hash{2}
	add(kept)
	other := add(dropped)
	qt.Assert(t, qt.IsTrue(waitFor(dropped, 1, 5*time.Second)))
	other.Drop()
	before := tr.announces(kept)
	qt.Check(t, qt.IsTrue(waitFor(kept, before+3, 6*time.Second)),
		qt.Commentf("%d announces after the drop", tr.announces(kept)-before))
}
