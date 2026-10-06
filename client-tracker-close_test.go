package torrent

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/tracker"
	"github.com/anacrolix/torrent/tracker/udp"
)

func TestClosingTheClientClosesItsTrackerSockets(t *testing.T) {
	var mu sync.Mutex
	var opened []net.PacketConn
	cfg := TestingConfig(t)
	cfg.DisableTrackers = false
	cfg.TrackerListenPacket = func(network, addr string) (net.PacketConn, error) {
		pc, err := net.ListenPacket(network, addr)
		if err == nil {
			mu.Lock()
			opened = append(opened, pc)
			mu.Unlock()
		}
		return pc, err
	}
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	to, _ := cl.AddTorrentInfoHash(metainfo.Hash{1})
	to.AddTrackers([][]string{{"udp://127.0.0.1:1/announce"}})
	cl.Close()
	mu.Lock()
	defer mu.Unlock()
	qt.Assert(t, qt.Not(qt.Equals(len(opened), 0)))
	for _, pc := range opened {
		qt.Check(t, qt.ErrorIs(pc.SetDeadline(time.Time{}), net.ErrClosed), qt.Commentf("%v after Close", pc.LocalAddr()))
	}
}

// A UDP tracker that keeps the events of the announces it is sent and answers each with one peer.
func newUDPEventTracker(t *testing.T) (url string, seen func() []tracker.AnnounceEvent) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	qt.Assert(t, qt.IsNil(err))
	t.Cleanup(func() { pc.Close() })
	var mu sync.Mutex
	var events []tracker.AnnounceEvent
	go func() {
		b := make([]byte, 0x10000)
		for {
			n, addr, err := pc.ReadFrom(b)
			if errors.Is(err, net.ErrClosed) {
				return
			}
			r := bytes.NewReader(b[:n])
			var h udp.RequestHeader
			if err != nil || udp.Read(r, &h) != nil {
				continue
			}
			var resp bytes.Buffer
			udp.Write(&resp, udp.ResponseHeader{Action: h.Action, TransactionId: h.TransactionId})
			switch h.Action {
			case udp.ActionConnect:
				udp.Write(&resp, udp.ConnectionResponse{ConnectionId: 1})
			case udp.ActionAnnounce:
				var req udp.AnnounceRequest
				if udp.Read(r, &req) != nil {
					continue
				}
				mu.Lock()
				events = append(events, req.Event)
				mu.Unlock()
				udp.Write(&resp, udp.AnnounceResponseHeader{Interval: 1})
				resp.Write([]byte{192, 0, 2, 1, 0x1a, 0xe1})
			default:
				continue
			}
			pc.WriteTo(resp.Bytes(), addr)
		}
	}()
	return "udp://" + pc.LocalAddr().String() + "/announce", func() []tracker.AnnounceEvent {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(events)
	}
}

func TestClosingTheClientSendsStoppedToItsUDPTracker(t *testing.T) {
	url, seen := newUDPEventTracker(t)
	cfg := TestingConfig(t)
	cfg.DisableTrackers = false
	cfg.DialForPeerConns = false
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	to, _ := cl.AddTorrentInfoHash(metainfo.Hash{1})
	to.AddTrackers([][]string{{url}})
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return to.Stats().PendingPeers == 1 })))
	// Dropped first, as by a caller that closes the client once it holds no torrent.
	to.Drop()
	cl.Close()
	qt.Check(t, qt.DeepEquals(seen(), []tracker.AnnounceEvent{tracker.Started, tracker.Stopped}))
}

func TestClosingTheClientAnnouncesNothingOnceItReturns(t *testing.T) {
	tr := &eventTracker{interval: "1"}
	cl, add := newEventTrackerClient(t, tr)
	to := add()
	qt.Assert(t, qt.IsTrue(eventually(func() bool { return to.Stats().PendingPeers == 1 })))
	cl.Close()
	qt.Check(t, qt.DeepEquals(tr.seen(), []string{"started", "stopped"}))
	time.Sleep(1500 * time.Millisecond)
	qt.Check(t, qt.DeepEquals(tr.seen(), []string{"started", "stopped"}))
}

// An HTTP tracker that keeps the events it is sent in order and answers each announce with one
// peer.
type eventTracker struct {
	interval string
	mu       sync.Mutex
	events   []string
}

func (me *eventTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	me.mu.Lock()
	me.events = append(me.events, r.URL.Query().Get("event"))
	me.mu.Unlock()
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
