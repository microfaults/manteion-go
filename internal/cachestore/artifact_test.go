package cachestore

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

func TestBuildArtifact_ContentAddressedAndIdempotent(t *testing.T) {
	s := New(t.TempDir())
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "k1", StatusCode: 200, Body: []byte("one")},
		{Key: "k2", StatusCode: 404, Body: []byte("two")},
	}
	if err := s.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	info, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	raw, err := os.ReadFile(info.Path)
	if err != nil {
		t.Fatalf("artifact file: %v", err)
	}
	sum := sha256.Sum256(raw)
	if info.ID != "sha256:"+hex.EncodeToString(sum[:]) || info.SizeBytes != int64(len(raw)) {
		t.Errorf("id/size = %s/%d, want sha256 of the %d file bytes", info.ID, info.SizeBytes, len(raw))
	}
	if info.Count != 2 || info.W5SHA256 != SetChecksum(entries) {
		t.Errorf("count/w5 = %d/%s", info.Count, info.W5SHA256)
	}
	// One WireEntry per line, decodable with the same type preload chunks carried.
	sc := bufio.NewScanner(strings.NewReader(string(raw)))
	var lines int
	for sc.Scan() {
		var e atroposdk.CacheBoxWireEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %d: %v", lines, err)
		}
		lines++
	}
	if lines != 2 {
		t.Errorf("lines = %d, want 2", lines)
	}
	again, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil || again.ID != info.ID || again.Path != info.Path {
		t.Errorf("rebuild = %+v, %v; want the same artifact", again, err)
	}
	if p, err := s.ArtifactPath(info.ID); err != nil || p != info.Path {
		t.Errorf("ArtifactPath = %s, %v", p, err)
	}
	for _, bad := range []string{"../../etc/passwd", "sha256:zz", "sha256:" + strings.Repeat("a", 63), ""} {
		if _, err := s.ArtifactPath(bad); err == nil {
			t.Errorf("ArtifactPath(%q) accepted", bad)
		}
	}
}

func TestBuildArtifact_RefusesEmptyRecording(t *testing.T) {
	s := New(t.TempDir())
	if _, err := s.BuildArtifact("exp-1", "phase-none", "frontend"); err == nil {
		t.Fatal("expected an error for a recording with no entries")
	}
}

// --- Pins beyond the brief's two tests ---------------------------------------

// seedAndBuild records a two-entry recording and builds its artifact.
func seedAndBuild(t *testing.T) (root string, s *Store, info ArtifactInfo) {
	t.Helper()
	root = t.TempDir()
	s = New(root)
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "k1", StatusCode: 200, Body: []byte("one")},
		{Key: "k2", StatusCode: 404, Body: []byte("two")},
	}
	if err := s.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	info, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	return root, s, info
}

// fileDigest returns the artifact id ("sha256:<hex>") of the file at path and its bytes.
func fileDigest(t *testing.T, path string) (string, []byte) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), raw
}

// leftoverTemps lists the build temp files still in the artifacts directory.
func leftoverTemps(t *testing.T, root string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(root, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".build-") {
			out = append(out, e.Name())
		}
	}
	return out
}

// The SDK's replay-set puller feeds each artifact line through the same staging
// as a /cachebox/preload/chunk entry, so every line must be byte-for-byte the
// entry JSON a preload chunk carries (HTML escaping, base64 bodies, header maps
// and timestamps included) with no envelope, and the W5 digest must be the one
// the SDK recomputes over the entries it parses back out.
func TestBuildArtifact_LinesAreThePreloadChunkWireBytes(t *testing.T) {
	full := atroposdk.CacheBoxWireEntry{
		Key:        "v2:aaa",
		StatusCode: 200,
		Header: map[string][]string{
			"Content-Type": {"text/html; charset=utf-8"},
			"X-Note":       {"<b>&amp;</b>", "caf\u00e9 \u2028"},
		},
		Body:               []byte("<html>a&b</html>"),
		ObservedLatencyUs:  1234,
		RecordedAt:         time.Date(2026, 10, 30, 12, 34, 56, 789000000, time.UTC),
		KeyStrategy:        "v2",
		StrategyVersion:    2,
		ResponseBodySHA256: "abc123",
	}
	// RequestMeta's type lives in an SDK-internal package, so decode it in.
	if err := json.Unmarshal([]byte(`{"request_meta":{"method":"GET","host":"cart:7070","path":"/cart?a=<b>&c=d"}}`), &full); err != nil {
		t.Fatal(err)
	}
	entries := []atroposdk.CacheBoxWireEntry{
		full,
		{Key: "v2:bbb", StatusCode: 404},
		{Key: "v2:ccc", StatusCode: 503, Body: []byte{0x00, 0x01, 0xff}},
	}
	s := New(t.TempDir())
	if err := s.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	info, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	_, raw := fileDigest(t, info.Path)

	// What the preload path puts on the wire for the same entries: the push
	// client json.Marshals the whole chunk request.
	chunk, err := json.Marshal(atroposdk.PreloadChunkRequest{ExperimentID: "exp-1", PhaseID: "phase-iso", ChunkSeq: 1, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(chunk, &body); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	for _, e := range body.Entries {
		want.Write(e)
		want.WriteByte('\n')
	}
	if !bytes.Equal(raw, want.Bytes()) {
		t.Errorf("artifact bytes differ from the entries of a preload chunk:\n got: %s\nwant: %s", raw, want.Bytes())
	}
	if !bytes.Contains(raw, []byte(`\u003c`)) {
		t.Errorf("HTML-special characters must stay escaped as in a chunk, got: %s", raw)
	}

	var parsed []atroposdk.CacheBoxWireEntry
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		var e atroposdk.CacheBoxWireEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %d: %v", len(parsed), err)
		}
		parsed = append(parsed, e)
	}
	if got := SetChecksum(parsed); got != info.W5SHA256 || info.Count != int64(len(parsed)) {
		t.Errorf("w5/count of the parsed lines = %s/%d, descriptor says %s/%d", got, len(parsed), info.W5SHA256, info.Count)
	}
}

// The id names the bytes, not the recording they came from: equal bytes share
// one artifact, more recorded data is a new artifact, and an artifact once
// built is never touched again.
func TestBuildArtifact_IDFollowsTheBytesNotTheRecording(t *testing.T) {
	s := New(t.TempDir())
	entries := []atroposdk.CacheBoxWireEntry{
		{Key: "k1", StatusCode: 200, Body: []byte("one")},
		{Key: "k2", StatusCode: 404, Body: []byte("two")},
	}
	if err := s.Append("exp-1", "phase-a", "frontend", entries); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("exp-2", "phase-b", "cart", entries); err != nil {
		t.Fatal(err)
	}
	a, err := s.BuildArtifact("exp-1", "phase-a", "frontend")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.BuildArtifact("exp-2", "phase-b", "cart")
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID || a.Path != b.Path {
		t.Errorf("identical recordings built %+v and %+v, want one shared artifact", a, b)
	}

	if err := s.Append("exp-1", "phase-a", "frontend", []atroposdk.CacheBoxWireEntry{{Key: "k3", StatusCode: 200}}); err != nil {
		t.Fatal(err)
	}
	c, err := s.BuildArtifact("exp-1", "phase-a", "frontend")
	if err != nil {
		t.Fatal(err)
	}
	if c.ID == a.ID || c.Path == a.Path || c.Count != 3 {
		t.Errorf("a grown recording built %+v, want a new artifact of 3 entries (earlier: %+v)", c, a)
	}
	if id, _ := fileDigest(t, a.Path); id != a.ID {
		t.Errorf("the earlier artifact changed on disk: %s, want %s", id, a.ID)
	}
}

func TestBuildArtifact_FileModeMatchesRecordings(t *testing.T) {
	_, _, info := seedAndBuild(t)
	st, err := os.Stat(info.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0o644 {
		t.Errorf("artifact mode = %v, want a regular 0644 file like the recordings", st.Mode())
	}
}

// A build whose file already exists leaves it alone, but only when its size is
// right: a file of any other size at the content-addressed name (a truncation,
// a torn write by some other writer) is not trusted and is replaced.
func TestBuildArtifact_RebuildKeepsAnIntactFileAndRepairsAWrongSizedOne(t *testing.T) {
	root, s, info := seedAndBuild(t)
	before, err := os.Stat(info.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BuildArtifact("exp-1", "phase-base", "frontend"); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(info.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Error("rebuilding replaced an intact artifact")
	}

	for name, mangle := range map[string]func() error{
		"truncated": func() error { return os.Truncate(info.Path, info.SizeBytes/2) },
		"empty":     func() error { return os.Truncate(info.Path, 0) },
		"overlong": func() error {
			f, err := os.OpenFile(info.Path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.WriteString("x")
			return err
		},
	} {
		if err := mangle(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		again, err := s.BuildArtifact("exp-1", "phase-base", "frontend")
		if err != nil || again != info {
			t.Fatalf("%s: rebuild = %+v, %v; want %+v", name, again, err, info)
		}
		if id, raw := fileDigest(t, info.Path); id != info.ID || int64(len(raw)) != info.SizeBytes {
			t.Errorf("%s: the file was not repaired: %s, %d bytes; want %s, %d bytes", name, id, len(raw), info.ID, info.SizeBytes)
		}
	}
	if left := leftoverTemps(t, root); len(left) != 0 {
		t.Errorf("temp files left in the artifacts dir: %v", left)
	}
}

// When the file cannot be installed the build fails (no descriptor for a file
// that cannot be served) and its temp file does not linger.
func TestBuildArtifact_FailedInstallReportsAnErrorAndLeavesNoTempFile(t *testing.T) {
	root, s, info := seedAndBuild(t)
	if err := os.Remove(info.Path); err != nil {
		t.Fatal(err)
	}
	// A non-empty directory at the content-addressed name: nothing can be renamed onto it.
	if err := os.MkdirAll(filepath.Join(info.Path, "occupied"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got, err := s.BuildArtifact("exp-1", "phase-base", "frontend"); err == nil {
		t.Fatalf("BuildArtifact = %+v, nil; want an error, %s is not a servable file", got, info.Path)
	}
	if left := leftoverTemps(t, root); len(left) != 0 {
		t.Errorf("temp files left in the artifacts dir: %v", left)
	}
}

// Concurrent builds of one recording agree on the artifact, and a reader racing
// them never sees part of the file: it is installed whole, by rename.
func TestBuildArtifact_ConcurrentBuildsNeverExposeAPartialFile(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	var entries []atroposdk.CacheBoxWireEntry
	for i := 0; i < 400; i++ {
		entries = append(entries, atroposdk.CacheBoxWireEntry{
			Key: fmt.Sprintf("v2:%04d", i), StatusCode: 200, Body: bytes.Repeat([]byte{byte(i)}, 4<<10),
		})
	}
	if err := s.Append("exp-1", "phase-base", "frontend", entries); err != nil {
		t.Fatal(err)
	}

	// The bytes and the name every build must converge on, derived without the store.
	var want bytes.Buffer
	enc := json.NewEncoder(&want)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	sum := sha256.Sum256(want.Bytes())
	wantID := "sha256:" + hex.EncodeToString(sum[:])
	wantPath := filepath.Join(root, "artifacts", hex.EncodeToString(sum[:])+".ndjson")

	const builders = 8
	infos := make([]ArtifactInfo, builders)
	errs := make([]error, builders)
	start := make(chan struct{})
	var builds sync.WaitGroup
	for i := 0; i < builders; i++ {
		builds.Add(1)
		go func() {
			defer builds.Done()
			<-start
			infos[i], errs[i] = s.BuildArtifact("exp-1", "phase-base", "frontend")
		}()
	}

	stop := make(chan struct{})
	var reader sync.WaitGroup
	var torn int
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if got, err := os.ReadFile(wantPath); err == nil && !bytes.Equal(got, want.Bytes()) {
				torn++
			}
		}
	}()

	close(start)
	builds.Wait()
	close(stop)
	reader.Wait()

	for i := range infos {
		if errs[i] != nil || infos[i].ID != wantID || infos[i].Path != wantPath {
			t.Errorf("build %d = %+v, %v; want id %s at %s", i, infos[i], errs[i], wantID, wantPath)
		}
	}
	if torn != 0 {
		t.Errorf("a reader saw a partial artifact %d times", torn)
	}
	if id, _ := fileDigest(t, wantPath); id != wantID {
		t.Errorf("final file = %s, want %s", id, wantID)
	}
	if left := leftoverTemps(t, root); len(left) != 0 {
		t.Errorf("temp files left in the artifacts dir: %v", left)
	}
}

// A refused build says which recording it refused and creates nothing.
func TestBuildArtifact_RefusalsNameTheRecordingAndLeaveNothingBehind(t *testing.T) {
	for name, tc := range map[string]struct {
		seed func(t *testing.T, s *Store, root string)
		want string
	}{
		"no recording": {
			seed: func(t *testing.T, s *Store, root string) {},
			want: "no recorded entries for (exp-1, phase-1, frontend)",
		},
		"recording with zero entries": {
			seed: func(t *testing.T, s *Store, root string) {
				if err := s.Append("exp-1", "phase-1", "frontend", nil); err != nil {
					t.Fatal(err)
				}
				// An empty file, not the no-recording case again.
				rec := filepath.Join(root, "experiments", "exp-1", "phases", "phase-1", "frontend.ndjson")
				if st, err := os.Stat(rec); err != nil || st.Size() != 0 {
					t.Fatalf("seeded recording = %v, %v; want an empty file", st, err)
				}
			},
			want: "no recorded entries for (exp-1, phase-1, frontend)",
		},
		"corrupt recording": {
			seed: func(t *testing.T, s *Store, root string) {
				dir := filepath.Join(root, "experiments", "exp-1", "phases", "phase-1")
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "frontend.ndjson"), []byte("{\"key\":\"k1\"}\nnot json\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: "(exp-1, phase-1, frontend)",
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			s := New(root)
			tc.seed(t, s, root)
			info, err := s.BuildArtifact("exp-1", "phase-1", "frontend")
			if err == nil {
				t.Fatalf("BuildArtifact = %+v, nil; want an error", info)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if name == "corrupt recording" {
				var syn *json.SyntaxError
				if !errors.As(err, &syn) {
					t.Errorf("error %q does not wrap the decode error", err)
				}
			}
			if _, err := os.Stat(filepath.Join(root, "artifacts")); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a refused build created the artifacts dir (stat: %v)", err)
			}
		})
	}
}

// ArtifactPath is the only way an id reaches the filesystem, so it accepts the
// canonical form and nothing else.
func TestArtifactPath_AcceptsOnlyCanonicalIDs(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	good := strings.Repeat("0123456789abcdef", 4)
	if got, err := s.ArtifactPath("sha256:" + good); err != nil || got != filepath.Join(root, "artifacts", good+".ndjson") {
		t.Errorf("ArtifactPath(canonical) = %q, %v", got, err)
	}
	for name, id := range map[string]string{
		"empty":                     "",
		"prefix only":               "sha256:",
		"63 digits":                 "sha256:" + good[:63],
		"65 digits":                 "sha256:" + good + "0",
		"uppercase hex":             "sha256:" + strings.ToUpper(good),
		"non-hex digit":             "sha256:" + good[:63] + "g",
		"no prefix":                 good,
		"wrong algorithm":           "sha1:" + good,
		"uppercase prefix":          "SHA256:" + good,
		"leading space":             " sha256:" + good,
		"trailing space":            "sha256:" + good + " ",
		"trailing newline":          "sha256:" + good + "\n",
		"embedded NUL":              "sha256:" + good[:32] + "\x00" + good[33:],
		"traversal suffix":          "sha256:" + good + "/../../etc/passwd",
		"traversal in place of hex": "sha256:" + strings.Repeat("../", 21) + ".",
		"slash in place of a digit": "sha256:" + good[:10] + "/" + good[11:],
		"backslash in the digits":   "sha256:" + good[:10] + `\` + good[11:],
		"non-ASCII digits":          "sha256:" + strings.Repeat("\u0663", 64),
	} {
		if got, err := s.ArtifactPath(id); err == nil {
			t.Errorf("%s: ArtifactPath(%q) = %q, want an error", name, id, got)
		}
	}
}
