package udp

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"
)

func TestAConnClientResolvesTheTrackerWithItsLookupHost(t *testing.T) {
	tracker, err := net.ListenPacket("udp4", "127.0.0.1:0")
	qt.Assert(t, qt.IsNil(err))
	defer tracker.Close()
	port := tracker.LocalAddr().(*net.UDPAddr).Port

	var asked []string
	cc, err := NewConnClient(NewConnClientOpts{
		Network: "udp4",
		Host:    net.JoinHostPort("tracker.tunnel.invalid", strconv.Itoa(port)),
		LookupHost: func(_ context.Context, host string) ([]string, error) {
			asked = append(asked, host)
			return []string{"127.0.0.1"}, nil
		},
	})
	qt.Assert(t, qt.IsNil(err))
	defer cc.Close()

	_, err = cc.Client.Writer.Write([]byte("hello"))
	qt.Assert(t, qt.IsNil(err))
	tracker.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 16)
	n, _, err := tracker.ReadFrom(buf)
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(string(buf[:n]), "hello"))
	qt.Check(t, qt.DeepEquals(asked, []string{"tracker.tunnel.invalid"}))
}
