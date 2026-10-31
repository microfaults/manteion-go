package cachestore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	artifactsDirName = "artifacts"
	artifactIDPrefix = "sha256:"
	artifactExt      = ".ndjson"
)

// artifactIDRe is the only id shape ArtifactPath maps to a file name. Go's $
// matches at the end of the text only, so "<id>\n" is rejected too.
var artifactIDRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ArtifactInfo describes a built replay-set artifact. The two digests are
// different on purpose: ID names the file bytes (what the SDK fetches by), while
// W5SHA256 is the wire spec §W5 digest over the entries (what the SDK verifies
// against the entries it staged when it commits the set).
type ArtifactInfo struct {
	ID        string // "sha256:<hex>" of the file bytes
	Path      string // {root}/artifacts/<hex>.ndjson
	Count     int64  // entries, one per line
	W5SHA256  string // SetChecksum over the entries
	SizeBytes int64  // length of the file
}

// BuildArtifact materializes the (exp, phase, service) recording as an immutable
// NDJSON file (one WireEntry per line, the lines preload chunks carried) and
// returns its descriptor. Idempotent: the same recording yields the same id and
// file. Errors: no recording / zero entries.
//
// Each line is encoding/json over the SDK's own entry type, as the recordings
// and the preload push path encode it, so a line is exactly the entry JSON a
// /cachebox/preload/chunk carries: no envelope, no second serialization.
//
// The file is written to a temp file in the artifacts directory, synced, and
// renamed onto its content-addressed name, so a reader never sees a partial
// artifact and concurrent or repeated builds converge on one path. A file
// already at that name is kept when its size matches the bytes just encoded; one
// of any other size is damaged and is replaced.
func (s *Store) BuildArtifact(experimentID, phaseID, service string) (ArtifactInfo, error) {
	entries, err := s.Read(experimentID, phaseID, service)
	if err != nil {
		return ArtifactInfo{}, fmt.Errorf("read recording (%s, %s, %s): %w", experimentID, phaseID, service, err)
	}
	if len(entries) == 0 {
		return ArtifactInfo{}, fmt.Errorf("no recorded entries for (%s, %s, %s)", experimentID, phaseID, service)
	}

	dir := s.artifactsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ArtifactInfo{}, err
	}
	tmp, err := os.CreateTemp(dir, ".build-*")
	if err != nil {
		return ArtifactInfo{}, err
	}
	// However this returns, the temp file is either renamed onto the artifact's
	// name below or removed here.
	installed := false
	defer func() {
		if !installed {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	// Hash and count the bytes as they are written; the buffer only saves a
	// write(2) per entry. Encode ends each value with the newline. An error from
	// Encode is the encoder's or, through the buffer, the file's.
	bw := bufio.NewWriterSize(tmp, 64<<10)
	h := sha256.New()
	var size countingWriter
	enc := json.NewEncoder(io.MultiWriter(bw, h, &size))
	for i := range entries {
		if err := enc.Encode(entries[i]); err != nil {
			return ArtifactInfo{}, fmt.Errorf("write entry %d of (%s, %s, %s): %w", i, experimentID, phaseID, service, err)
		}
	}
	if err := bw.Flush(); err != nil {
		return ArtifactInfo{}, err
	}

	hexsum := hex.EncodeToString(h.Sum(nil))
	info := ArtifactInfo{
		ID:        artifactIDPrefix + hexsum,
		Path:      s.artifactFile(hexsum),
		Count:     int64(len(entries)),
		W5SHA256:  SetChecksum(entries),
		SizeBytes: size.n,
	}

	// The name is the digest of the bytes, so a file already there holds them
	// unless it was damaged; its size is the cheap check for that.
	if st, err := os.Stat(info.Path); err == nil && st.Mode().IsRegular() && st.Size() == info.SizeBytes {
		return info, nil
	}

	// Data on disk before the name exists, then the atomic rename. CreateTemp
	// makes 0600; the recordings are 0644, so the artifact matches them.
	if err := tmp.Sync(); err != nil {
		return ArtifactInfo{}, err
	}
	if err := tmp.Chmod(0o644); err != nil {
		return ArtifactInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return ArtifactInfo{}, err
	}
	if err := os.Rename(tmp.Name(), info.Path); err != nil {
		return ArtifactInfo{}, err
	}
	installed = true
	return info, nil
}

// ArtifactPath maps a valid id ("sha256:" + 64 hex) to its file path; any other
// id is an error (never a path traversal). It does not touch the filesystem, so
// the file may not exist.
func (s *Store) ArtifactPath(id string) (string, error) {
	if !artifactIDRe.MatchString(id) {
		// %.80q: the id comes from outside and may be huge.
		return "", fmt.Errorf("invalid artifact id %.80q: want %q followed by 64 lowercase hex digits", id, artifactIDPrefix)
	}
	return s.artifactFile(strings.TrimPrefix(id, artifactIDPrefix)), nil
}

func (s *Store) artifactsDir() string {
	return filepath.Join(s.root, artifactsDirName)
}

// artifactFile is the one place a file name is derived from a digest, so
// BuildArtifact and ArtifactPath cannot disagree about where an artifact lives.
// Callers pass 64 lowercase hex digits only.
func (s *Store) artifactFile(hexsum string) string {
	return filepath.Join(s.artifactsDir(), hexsum+artifactExt)
}

// countingWriter counts the bytes written through it.
type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}
