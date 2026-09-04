package storefront

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// web holds the browser storefront: a dependency-free single-page shop.
//
// It exists because the rest of the demo has no browser surface at all — the
// service named "web-client" is the same Go binary running ROLE=loadgen, a
// headless net/http client. A real page is what turns an injected fault into
// something a person watching the demo actually sees.
//
// Embedding the assets keeps the project's one-Go-module / one-image shape and,
// because the UI is served by storefront-bff itself, it is same-origin with the
// API and needs no CORS anywhere.
//
//go:embed web/index.html web/app.js web/style.css
var webFS embed.FS

// uiHandler serves the storefront: "/" returns the shell, and the static assets
// come straight from the embedded filesystem.
type uiHandler struct {
	index  []byte
	assets http.Handler
	// started is the process start time, used as the asset Last-Modified so
	// browsers can cache app.js and style.css across a session.
	started time.Time
}

// assistPlaceholder is substituted in the shell at startup. Doing it once here
// rather than exposing a /api/features endpoint keeps page loads from adding an
// HTTPPath entity and traffic to storefront-bff's baseline.
const assistPlaceholder = "__ASSIST_ENABLED__"

func newUIHandler(assistEnabled bool) (http.Handler, error) {
	index, err := webFS.ReadFile("web/index.html")
	if err != nil {
		return nil, fmt.Errorf("read storefront shell: %w", err)
	}

	if !bytes.Contains(index, []byte(assistPlaceholder)) {
		// A silent no-op here would ship a shell containing the literal
		// placeholder, which is a JavaScript syntax error and blanks the page.
		return nil, fmt.Errorf("storefront shell is missing %s", assistPlaceholder)
	}
	index = bytes.ReplaceAll(index, []byte(assistPlaceholder),
		[]byte(strconv.FormatBool(assistEnabled)))

	assets, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, fmt.Errorf("sub storefront assets: %w", err)
	}

	return &uiHandler{
		index:   index,
		assets:  http.FileServer(http.FS(assets)),
		started: time.Now(),
	}, nil
}

func (h *uiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/app.js", "/style.css":
		// Served with a short cache lifetime: long enough that clicking around
		// does not refetch them, short enough that a redeploy is picked up.
		w.Header().Set("Cache-Control", "public, max-age=300")
		h.assets.ServeHTTP(w, r)
		return
	}

	// Everything else renders the shell. The client router owns /product/{id},
	// /cart, /checkout and /order/{id}, so a deep link or a refresh on any of
	// them has to return the same HTML rather than a 404.
	//
	// Unmatched /api/* paths are the one exception — returning HTML there would
	// turn a wrong API call into a confusing 200.
	if strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The shell carries the deep-link routes, so a redeploy must be picked up
	// immediately rather than served from a browser cache.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.index)
}
