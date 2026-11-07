package api

import (
	"context"
	"errors"
	"net/http"
	"os"
	"regexp"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// artifactReader is the artifact handler's view of the replay-artifact store:
// it resolves an artifact id to the row naming the file. Injected as an
// interface so handler tests can supply rows without a database.
type artifactReader interface {
	GetByID(ctx context.Context, id string) (*model.ReplayArtifact, error)
}

// artifactIDRe is the only id the endpoint looks up: "sha256:" and 64 lowercase
// hex digits, the shape cachestore.ArtifactPath maps to a file name (the
// handler's test pins the two to agree). Go's $ matches at the end of the text
// only, so "<id>\n" is rejected too. The mux hands the handler whatever a client
// percent-encodes into the segment, a slash or a newline included, so this
// check is the handler's.
var artifactIDRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// handleCacheArtifact serves an immutable replay-set artifact.
//
// An id that is not in the canonical shape is a 404, the same as an unknown
// one, answered before the store is asked. The file is the one the row names;
// it is opened for this request only and served through http.ServeContent,
// which answers HEAD, Range (206, or 416 past the end) and If-None-Match (304).
// Errors are written before the ETag and the immutable Cache-Control are set, so
// no cache keeps a failure under them.
//
// @Summary      Fetch a replay-set artifact
// @Description  Immutable NDJSON replay set (one CacheBoxWireEntry per line) named by
// @Description  RuleSync.replay_set.id ("sha256:<hex>" of the file bytes). Supports
// @Description  HEAD (Content-Length, ETag) and Range (206); the instance fetches it in
// @Description  ≤4 MiB ranges and commits when count and the W5 digest match.
// @Tags         cache
// @Produce      application/x-ndjson
// @Param        id             path    string  true   "artifact id (sha256:<hex>)"
// @Param        Range          header  string  false  "bytes=a-b; answered 206, or 416 past the end"
// @Param        If-None-Match  header  string  false  "the artifact's ETag; a match answers 304"
// @Success      200  {string}  binary  "the artifact; headers ETag, Content-Length"
// @Success      206  {string}  binary  "the requested byte range; headers ETag, Content-Range"
// @Success      304  "unchanged; the artifact is immutable"
// @Failure      404  {object}  api.ErrorResponse  "unknown artifact (an id that is not `sha256:` followed by 64 lowercase hex digits is treated as unknown)"
// @Failure      416  "range not satisfiable"
// @Failure      500  {object}  api.ErrorResponse  "artifact lookup failed, or its file is unavailable"
// @Router       /cache/artifacts/{id} [get]
func (s *Server) handleCacheArtifact(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !artifactIDRe.MatchString(id) {
		writeError(w, http.StatusNotFound, "artifact not found")
		return
	}
	a, err := s.artifacts.GetByID(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "artifact not found")
		return
	}
	if err != nil {
		s.logger.Error("artifact: lookup failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	f, err := os.Open(a.Path)
	if err != nil {
		s.logger.Error("artifact: file unavailable", "id", id, "path", a.Path, "error", err)
		writeError(w, http.StatusInternalServerError, "artifact file unavailable")
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		// A directory opens and stats, then ServeContent would promise its
		// size and fail to read it: a 200 with no bytes.
		err = errors.New("not a regular file")
	}
	if err != nil {
		s.logger.Error("artifact: file unavailable", "id", id, "path", a.Path, "error", err)
		writeError(w, http.StatusInternalServerError, "artifact file unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("ETag", `"`+a.ID+`"`)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeContent(w, r, "", st.ModTime(), f) // HEAD, Range (206/416), If-None-Match (304)
}
