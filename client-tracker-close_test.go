package torrent

import (
	"net"
	"sync"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/metainfo"
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
