package torrent

import (
	"bytes"
	"errors"
	"net"
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
