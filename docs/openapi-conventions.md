# OpenAPI Annotation Conventions

All Go handlers in this repo use [swaggo/swag](https://github.com/swaggo/swag) v2 to generate OpenAPI 3.1.

## Toolchain

- `swag` v2.0.0-rc5 (pinned; v2.0.0 has not been released yet). Install with `go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5`. Note: `swag --version` self-reports `v2.0.0` regardless of which v2 RC is installed; trust the `go install` command pin (`@v2.0.0-rc5`), not the binary's banner.
- The general-info annotations use swag v2's `@servers.url` / `@servers.description` directives (paired by ordinal position) rather than the deprecated `@host` / `@BasePath` / `@schemes` triplet. Re-evaluate the syntax once swag publishes a stable v2.0.0.
- The post-processor at `scripts/strip-empty-externaldocs.py` strips empty `externalDocs:` blocks emitted by swag v2.0.x; remove this once swag publishes a release that omits empty stubs.
- Specs are committed at `docs/swagger.{yaml,json}` and validated in CI on pushes to `main`/`develop`.

## Required annotations on every handler

- `@Summary` — one-line imperative ("Create rule", "List workflows")
- `@Description` — one paragraph; UI renders this as JSDoc
- `@Tags` — one of: `rules`, `autorules`, `faults`, `sdk`, `experiments`, `workflows`, `zeus-proxy`, `health`, `admin`
- `@Produce json` (or `text/event-stream` for SSE)
- `@Accept json` (for routes that take a body)
- `@Success` and `@Failure` for every documented status code
- `@Router` with method in brackets

## Body and response types

- Use Go model types directly: `{object} model.Rule`, `{array} model.Workflow`
- Use `api.ErrorResponse` for all 4xx/5xx responses
- For path/query params, use `@Param name in type required "description"`

## Streaming endpoints (SSE)

- `@Produce text/event-stream`
- Event types listed in the description text
- See run-events handler (when added) for the canonical example

## Regenerating the spec

Run `make openapi` from the repo root.

Commit `docs/swagger.{yaml,json}` together with handler edits. CI fails if the spec is stale.
