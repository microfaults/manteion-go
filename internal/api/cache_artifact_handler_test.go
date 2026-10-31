package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/cachestore"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeArtifacts stands in for the replay-artifact store. It records every
// lookup, so a test can tell an id answered 404 without touching the store
// from one the store was asked about and did not know.
type fakeArtifacts struct {
	byID    map[string]*model.ReplayArtifact
	err     error    // returned by every lookup when set
	lookups []string // the ids asked for, in order
}

func (f *fakeArtifacts) GetByID(_ context.Context, id string) (*model.ReplayArtifact, error) {
	f.lookups = append(f.lookups, id)
	if f.err != nil {
		return nil, f.err
	}
	if a, ok := f.byID[id]; ok {
		return a, nil
	}
	return nil, store.ErrNotFound
}

// The two rows artifactServer knows: one whose file exists, one whose file is
// gone. Both ids have the canonical shape, so they reach the store.
var (
	artifactTestID = "sha256:" + strings.Repeat("0123456789abcdef", 4)
	artifactGoneID = "sha256:" + strings.Repeat("f", 64)
)

func artifactServer(t *testing.T) (*Server, string, []byte) {
	t.Helper()
	body := []byte("{\"key\":\"k1\"}\n{\"key\":\"k2\"}\n")
	path := filepath.Join(t.TempDir(), "a.ndjson")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	id := artifactTestID
	s := &Server{logger: discardLogger(), artifacts: &fakeArtifacts{byID: map[string]*model.ReplayArtifact{
		id:             {ID: id, Path: path, SizeBytes: int64(len(body))},
		artifactGoneID: {ID: artifactGoneID, Path: filepath.Join(t.TempDir(), "missing.ndjson")},
	}}}
	return s, id, body
}

// lookupsOf returns the ids the server's fake store was asked about.
func lookupsOf(s *Server) []string { return s.artifacts.(*fakeArtifacts).lookups }

func getArtifact(t *testing.T, s *Server, method, id string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.routes(mux)
	req := httptest.NewRequest(method, "/api/v1/cache/artifacts/"+id, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestHandleCacheArtifact(t *testing.T) {
	s, id, body := artifactServer(t)

	w := getArtifact(t, s, http.MethodGet, id, nil)
	if w.Code != http.StatusOK || w.Body.String() != string(body) {
		t.Fatalf("GET: %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("ETag") != `"`+id+`"` || w.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Errorf("GET headers = %v", w.Header())
	}

	w = getArtifact(t, s, http.MethodHead, id, nil)
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("Content-Length") != "26" || w.Header().Get("ETag") == "" {
		t.Errorf("HEAD: %d len=%d headers=%v", w.Code, w.Body.Len(), w.Header())
	}

	w = getArtifact(t, s, http.MethodGet, id, map[string]string{"Range": "bytes=0-12"})
	if w.Code != http.StatusPartialContent || w.Body.String() != string(body[:13]) || w.Header().Get("Content-Range") != "bytes 0-12/26" {
		t.Errorf("Range: %d %q %v", w.Code, w.Body.String(), w.Header())
	}
	if w = getArtifact(t, s, http.MethodGet, id, map[string]string{"Range": "bytes=999-"}); w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("Range past EOF: %d, want 416", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, id, map[string]string{"If-None-Match": `"` + id + `"`}); w.Code != http.StatusNotModified {
		t.Errorf("If-None-Match: %d, want 304", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, "sha256:unknown", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown: %d, want 404", w.Code)
	}
	if w = getArtifact(t, s, http.MethodGet, artifactGoneID, nil); w.Code != http.StatusInternalServerError {
		t.Errorf("missing file: %d, want 500 (never a 200 with an empty body)", w.Code)
	}
}

// The success headers: the immutable caching policy rides every answer that
// carries artifact bytes, Last-Modified is the file's own modtime (what
// ServeContent derives its conditional answers from), and a conditional GET
// with another entity tag is a full answer, not a 304.
func TestHandleCacheArtifact_SuccessHeaders(t *testing.T) {
	s, id, body := artifactServer(t)
	const immutable = "public, max-age=31536000, immutable"
	fi, err := os.Stat(s.artifacts.(*fakeArtifacts).byID[id].Path)
	if err != nil {
		t.Fatal(err)
	}
	modtime := fi.ModTime().UTC().Format(http.TimeFormat)

	for name, c := range map[string]struct {
		method string
		hdr    map[string]string
		code   int
	}{
		"GET":       {http.MethodGet, nil, http.StatusOK},
		"HEAD":      {http.MethodHead, nil, http.StatusOK},
		"Range":     {http.MethodGet, map[string]string{"Range": "bytes=0-12"}, http.StatusPartialContent},
		"other tag": {http.MethodGet, map[string]string{"If-None-Match": `"sha256:other"`}, http.StatusOK},
	} {
		w := getArtifact(t, s, c.method, id, c.hdr)
		h := w.Header()
		if w.Code != c.code {
			t.Errorf("%s: status %d, want %d", name, w.Code, c.code)
		}
		if h.Get("Cache-Control") != immutable || h.Get("Accept-Ranges") != "bytes" || h.Get("Last-Modified") != modtime {
			t.Errorf("%s: Cache-Control=%q Accept-Ranges=%q Last-Modified=%q, want %q, bytes, %q",
				name, h.Get("Cache-Control"), h.Get("Accept-Ranges"), h.Get("Last-Modified"), immutable, modtime)
		}
		if h.Get("ETag") != `"`+id+`"` || h.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("%s: ETag=%q Content-Type=%q", name, h.Get("ETag"), h.Get("Content-Type"))
		}
		if name == "other tag" && w.Body.String() != string(body) {
			t.Errorf("%s: body %q, want the whole artifact", name, w.Body.String())
		}
	}

	w := getArtifact(t, s, http.MethodGet, id, map[string]string{"If-None-Match": `"` + id + `"`})
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 || w.Header().Get("ETag") != `"`+id+`"` {
		t.Errorf("If-None-Match: %d len=%d ETag=%q, want an empty 304 naming the tag", w.Code, w.Body.Len(), w.Header().Get("ETag"))
	}
}

// {id} reaches the store only in the canonical shape, "sha256:" and 64 lowercase
// hex digits, the shape cachestore.ArtifactPath maps to a file name. The mux
// hands the handler whatever a client percent-encodes into the segment (a
// slash, a backslash, a newline), so the check is the handler's, and it comes
// before the lookup: anything else is a 404 the store never hears about.
func TestHandleCacheArtifact_ChecksTheIDBeforeAnyLookup(t *testing.T) {
	hex64 := strings.TrimPrefix(artifactTestID, "sha256:")
	unknown := "sha256:" + strings.Repeat("a", 64)
	cs := cachestore.New(t.TempDir())

	cases := []struct {
		name  string
		id    string // as it appears in the request path
		valid bool   // reaches the store
	}{
		{"known id", artifactTestID, true},
		{"canonical but unknown", unknown, true},
		{"colon percent-encoded", "sha256%3A" + hex64, true},

		{"not a digest", "sha256:unknown", false},
		{"uppercase hex", "sha256:" + strings.ToUpper(hex64), false},
		{"63 digits", "sha256:" + hex64[:63], false},
		{"65 digits", "sha256:" + hex64 + "0", false},
		{"no prefix", hex64, false},
		{"junk before the prefix", "x" + artifactTestID, false},
		{"slash before a valid id", "..%2F" + artifactTestID, false},
		{"another algorithm", "sha1:" + hex64, false},
		{"empty digest", "sha256:", false},
		{"non-hex digit", "sha256:g" + hex64[1:], false},
		{"slash after a valid id", artifactTestID + "%2F..%2F..%2Fetc%2Fpasswd", false},
		{"slashes for a digest", "sha256:..%2F..%2F..%2Fetc%2Fpasswd", false},
		{"backslash", "sha256:..%5C..%5Cwindows%5Cwin.ini", false},
		{"trailing newline", artifactTestID + "%0A", false},
		{"embedded NUL", "sha256:" + hex64[:63] + "%00", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, _ := artifactServer(t)
			decoded, err := url.PathUnescape(c.id)
			if err != nil {
				t.Fatal(err)
			}
			// cachestore.ArtifactPath is where the rule comes from; the handler's own copy must agree with it on this id.
			if _, pathErr := cs.ArtifactPath(decoded); (pathErr == nil) != c.valid {
				t.Fatalf("cachestore.ArtifactPath(%q) error = %v, but the case says valid = %v", decoded, pathErr, c.valid)
			}

			w := getArtifact(t, s, http.MethodGet, c.id, nil)
			if !c.valid {
				if w.Code != http.StatusNotFound || w.Header().Get("Content-Type") != "application/json" ||
					strings.TrimSpace(w.Body.String()) != `{"error":"artifact not found"}` {
					t.Errorf("answer: %d %v %q, want the handler's 404 envelope", w.Code, w.Header(), w.Body.String())
				}
				if got := lookupsOf(s); len(got) != 0 {
					t.Errorf("store was asked for %q, want no lookup", got)
				}
				return
			}
			if got := lookupsOf(s); len(got) != 1 || got[0] != decoded {
				t.Errorf("store was asked for %q, want exactly %q", got, decoded)
			}
			want := http.StatusNotFound
			if decoded == artifactTestID {
				want = http.StatusOK
			}
			if w.Code != want {
				t.Errorf("status %d, want %d", w.Code, want)
			}
		})
	}
}

// Every way the endpoint refuses is a status the puller reads correctly (5xx is
// retried from the same offset, any other 4xx ends the chunk for good) and none
// of them carries the artifact's tag or its year-long immutable policy: a cache
// that stored a 500 for a missing file under that policy would keep serving it
// after the file is rebuilt.
func TestHandleCacheArtifact_RefusalsAreTheRightStatusAndNeverCacheable(t *testing.T) {
	unknown := "sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name  string
		setup func(t *testing.T, s *Server)
		id    string
		hdr   map[string]string
		code  int
		msg   string // the {"error": msg} envelope; empty when the body is not one
	}{
		{name: "unknown id", id: unknown, code: 404, msg: "artifact not found"},
		{name: "malformed id", id: "sha256:unknown", code: 404, msg: "artifact not found"},
		{name: "file gone", id: artifactGoneID, code: 500, msg: "artifact file unavailable"},
		{
			name: "lookup fails, which is not a miss",
			setup: func(_ *testing.T, s *Server) {
				s.artifacts.(*fakeArtifacts).err = errors.New("connection refused")
			},
			id: artifactTestID, code: 500, msg: "lookup failed",
		},
		{
			name: "path is a directory",
			setup: func(t *testing.T, s *Server) {
				s.artifacts.(*fakeArtifacts).byID[artifactTestID].Path = t.TempDir()
			},
			id: artifactTestID, code: 500, msg: "artifact file unavailable",
		},
		{name: "range past the end", id: artifactTestID, hdr: map[string]string{"Range": "bytes=999-"}, code: 416},
		{name: "range at the end", id: artifactTestID, hdr: map[string]string{"Range": "bytes=26-"}, code: 416},
	}
	for _, c := range cases {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(c.name+"/"+method, func(t *testing.T) {
				s, _, _ := artifactServer(t)
				if c.setup != nil {
					c.setup(t, s)
				}
				w := getArtifact(t, s, method, c.id, c.hdr)
				if w.Code != c.code {
					t.Fatalf("status %d (body %q), want %d", w.Code, w.Body.String(), c.code)
				}
				if w.Header().Get("Cache-Control") != "" || w.Header().Get("ETag") != "" {
					t.Errorf("Cache-Control=%q ETag=%q on a %d, want neither", w.Header().Get("Cache-Control"), w.Header().Get("ETag"), c.code)
				}
				if method == http.MethodGet && c.msg != "" && strings.TrimSpace(w.Body.String()) != `{"error":"`+c.msg+`"}` {
					t.Errorf("body %q, want the %q error envelope", w.Body.String(), c.msg)
				}
				if c.code == 416 && w.Header().Get("Content-Range") != "bytes */26" {
					t.Errorf("Content-Range = %q, want %q", w.Header().Get("Content-Range"), "bytes */26")
				}
			})
		}
	}
}

// pullArtifact fetches id the way the SDK's replay-set puller does: GET with
// Range: bytes=off-(off+chunk-1) from offset 0. A 206 advances by its body and
// ends the pull when the numeric total of its Content-Range is reached or the
// body is short; a 416 after the first chunk ends it too (the size was an exact
// multiple of the chunk). Any other answer fails the test.
func pullArtifact(t *testing.T, s *Server, id string, chunk int64) []byte {
	t.Helper()
	var got []byte
	for off := int64(0); ; {
		w := getArtifact(t, s, http.MethodGet, id, map[string]string{
			"Range": fmt.Sprintf("bytes=%d-%d", off, off+chunk-1),
		})
		switch {
		case w.Code == http.StatusPartialContent:
			cr := w.Header().Get("Content-Range") // "bytes a-b/total"
			total, err := strconv.ParseInt(cr[strings.LastIndexByte(cr, '/')+1:], 10, 64)
			if err != nil {
				t.Fatalf("chunk %d: Content-Range %q has no numeric total", off, cr)
			}
			got = append(got, w.Body.Bytes()...)
			off += int64(w.Body.Len())
			if off >= total || int64(w.Body.Len()) < chunk {
				return got
			}
		case w.Code == http.StatusRequestedRangeNotSatisfiable && off > 0:
			return got
		default:
			t.Fatalf("chunk at %d (size %d): status %d %q", off, chunk, w.Code, w.Body.String())
		}
	}
}

// The puller's whole contract against the fixture, at chunk sizes that split the
// 26-byte file mid-line (1, 5), exactly in half (13), whole (26), more than the
// file (27) and at the SDK's real 4 MiB.
func TestHandleCacheArtifact_PullerChunkLoopReassemblesTheFile(t *testing.T) {
	s, id, body := artifactServer(t)
	for _, chunk := range []int64{1, 5, 13, 26, 27, 4 << 20} {
		if got := pullArtifact(t, s, id, chunk); !bytes.Equal(got, body) {
			t.Errorf("chunk %d: pulled %q, want %q", chunk, got, body)
		}
	}
}

// What BuildArtifact wrote is what is served, byte for byte: the row carries the
// builder's Path, which is where ArtifactPath maps the id, and the bytes pulled
// in Range chunks hash to that id.
func TestHandleCacheArtifact_ServesWhatBuildArtifactWrote(t *testing.T) {
	cs := cachestore.New(t.TempDir())
	var entries []atroposdk.CacheBoxWireEntry
	for i := range 40 {
		entries = append(entries, atroposdk.CacheBoxWireEntry{
			Key: fmt.Sprintf("k%02d", i), StatusCode: 200, Body: []byte(strings.Repeat("x", 10*i)),
		})
	}
	if err := cs.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	info, err := cs.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	if p, err := cs.ArtifactPath(info.ID); err != nil || p != info.Path {
		t.Fatalf("the row's path %q is not ArtifactPath(%q) = %q, %v", info.Path, info.ID, p, err)
	}
	s := &Server{logger: discardLogger(), artifacts: &fakeArtifacts{byID: map[string]*model.ReplayArtifact{
		info.ID: {ID: info.ID, ExperimentID: "exp-1", PhaseID: "phase-base", Service: "frontend",
			EntryCount: info.Count, W5SHA256: info.W5SHA256, SizeBytes: info.SizeBytes, Path: info.Path},
	}}}

	whole := getArtifact(t, s, http.MethodGet, info.ID, nil)
	if whole.Code != http.StatusOK || int64(whole.Body.Len()) != info.SizeBytes {
		t.Fatalf("GET: %d, %d bytes, want 200 and %d", whole.Code, whole.Body.Len(), info.SizeBytes)
	}
	head := getArtifact(t, s, http.MethodHead, info.ID, nil)
	if head.Header().Get("Content-Length") != strconv.FormatInt(info.SizeBytes, 10) {
		t.Errorf("HEAD Content-Length = %q, want %d", head.Header().Get("Content-Length"), info.SizeBytes)
	}
	for _, chunk := range []int64{4 << 20, 1000, info.SizeBytes} {
		got := pullArtifact(t, s, info.ID, chunk)
		sum := sha256.Sum256(got)
		if "sha256:"+hex.EncodeToString(sum[:]) != info.ID || !bytes.Equal(got, whole.Body.Bytes()) {
			t.Errorf("chunk %d: %d bytes pulled, sha256 %x, want the %d bytes of %s", chunk, len(got), sum, info.SizeBytes, info.ID)
		}
	}
	if n := int64(bytes.Count(whole.Body.Bytes(), []byte("\n"))); n != info.Count {
		t.Errorf("lines = %d, want the artifact's %d entries", n, info.Count)
	}
}
