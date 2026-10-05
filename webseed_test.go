package torrent

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/internal/testutil"
)

// Tests that the client can download a multi-file torrent from two webseeds simultaneously when
// each webseed only has one of the two files (non-overlapping data). When a webseed receives a 404
// for a file it doesn't have, the pieces for that file are removed from its bitmap and the
// scheduler reassigns them to the other webseed.
func TestDownloadFromTwoNonOverlappingWebseeds(t *testing.T) {
	const pieceLen = 2 * defaultChunkSize // 32 KiB; two 16 KiB chunks per piece

	// Two files, each spanning exactly 2 pieces.
	fileLen := 2 * pieceLen
	dataA := make([]byte, fileLen)
	dataB := make([]byte, fileLen)
	rand.Read(dataA)
	rand.Read(dataB)

	tu := testutil.Torrent{
		Name: "testdata",
		Files: []testutil.File{
			{Name: "a.bin", Data: string(dataA)},
			{Name: "b.bin", Data: string(dataB)},
		},
	}
	mi, _ := tu.Generate(int64(pieceLen))

	// Server 1: serves only a.bin; b.bin returns 404 naturally.
	dir1 := t.TempDir()
	qt.Assert(t, qt.IsNil(os.MkdirAll(filepath.Join(dir1, "testdata"), 0o755)))
	qt.Assert(t, qt.IsNil(os.WriteFile(filepath.Join(dir1, "testdata", "a.bin"), dataA, 0o644)))
	srv1 := httptest.NewServer(http.FileServer(http.Dir(dir1)))
	defer srv1.Close()

	// Server 2: serves only b.bin; a.bin returns 404 naturally.
	dir2 := t.TempDir()
	qt.Assert(t, qt.IsNil(os.MkdirAll(filepath.Join(dir2, "testdata"), 0o755)))
	qt.Assert(t, qt.IsNil(os.WriteFile(filepath.Join(dir2, "testdata", "b.bin"), dataB, 0o644)))
	srv2 := httptest.NewServer(http.FileServer(http.Dir(dir2)))
	defer srv2.Close()

	cfg := TestingConfig(t)
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer cl.Close()

	// BEP 19 multi-file webseeds use a trailing slash; the file path is appended automatically.
	tt, _, err := cl.AddTorrentSpec(&TorrentSpec{
		AddTorrentOpts: AddTorrentOpts{
			InfoHash:  mi.HashInfoBytes(),
			InfoBytes: mi.InfoBytes,
		},
		Webseeds: []string{srv1.URL + "/", srv2.URL + "/"},
	})
	qt.Assert(t, qt.IsNil(err))

	tt.DownloadAll()
	qt.Assert(t, qt.IsTrue(cl.WaitAll()))

	r := tt.NewReader()
	defer r.Close()
	got, err := io.ReadAll(r)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.DeepEquals(got, append(dataA, dataB...)))
}

// slowWriter sends a response a chunk at a time with a pause between, like a
// webseed slower than the reader that wants its bytes.
type slowWriter struct {
	http.ResponseWriter
}

func (w slowWriter) Write(p []byte) (n int, err error) {
	for len(p) > 0 {
		k := min(len(p), defaultChunkSize)
		m, err := w.ResponseWriter.Write(p[:k])
		n += m
		if err != nil {
			return n, err
		}
		p = p[k:]
		time.Sleep(120 * time.Millisecond)
	}
	return n, nil
}

// A reader that seeks ahead of the request already reading its slice from the
// only webseed gets its bytes from a request at its own position, rather than
// once the slice's request has read its way there.
func TestWebseedServesAReaderAheadOfTheSliceRequestFirst(t *testing.T) {
	const pieceLen = 256 << 10
	data := make([]byte, 8<<20)
	rand.Read(data)
	tu := testutil.Torrent{Name: "film.bin", Files: []testutil.File{{Data: string(data)}}}
	mi, _ := tu.Generate(pieceLen)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(slowWriter{w}, r, "film.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	cl, err := NewClient(TestingConfig(t))
	qt.Assert(t, qt.IsNil(err))
	defer cl.Close()
	tt, _, err := cl.AddTorrentSpec(&TorrentSpec{
		AddTorrentOpts: AddTorrentOpts{
			InfoHash:  mi.HashInfoBytes(),
			InfoBytes: mi.InfoBytes,
		},
		Webseeds: []string{srv.URL + "/film.bin"},
	})
	qt.Assert(t, qt.IsNil(err))
	tt.DownloadAll()
	for tt.BytesCompleted() < 768<<10 {
		time.Sleep(10 * time.Millisecond)
	}

	r := tt.NewReader()
	defer r.Close()
	r.SetResponsive()
	const at = 6 << 20
	_, err = r.Seek(at, io.SeekStart)
	qt.Assert(t, qt.IsNil(err))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	buf := make([]byte, 1024)
	n, err := r.ReadContext(ctx, buf)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.DeepEquals(buf[:n], data[at:at+n]))
}
