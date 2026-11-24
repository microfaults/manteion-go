# OpenAPI Annotation Conventions

All Go handlers in this repo use [swaggo/swag](https://github.com/swaggo/swag) v2 to generate OpenAPI 3.1.

## Toolchain

- `swag` v2.0.0-rc6 (pinned; v2.0.0 has not been released yet). Install with `go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc6`, using Go 1.27.3 or newer: swag parses the standard library of the Go it was built with, and `go.mod` says `go 1.27.3`. Note: `swag --version` self-reports `v2.0.0` regardless of which v2 RC is installed; trust the `go install` command pin (`@v2.0.0-rc6`), not the binary's banner (`go version -m $(which swag)` shows both the swag version and the Go that built it).
- The general-info annotations use swag v2's `@servers.url` / `@servers.description` directives (paired by ordinal position) rather than the deprecated `@host` / `@BasePath` / `@schemes` triplet. Re-evaluate the syntax once swag publishes a stable v2.0.0.
- The post-processor at `scripts/strip-empty-externaldocs.py` strips empty `externalDocs:` blocks emitted by swag v2.0.x; remove this once swag publishes a release that omits empty stubs.
- Specs are committed at `docs/swagger.{yaml,json}` and validated in CI on pushes to `main`/`develop`.

## Required annotations on every handler

- `@Summary` — one-line imperative ("Create rule", "List workflows")
- `@Description` — one paragraph; UI renders this as JSDoc
- `@Tags` — one of: `rules`, `autorules`, `faults`, `sdk`, `experiments`, `workflows`, `zeus-proxy`, `health`, `admin`, `cache`
- `@Produce json` (or `text/event-stream` for SSE)
- `@Accept json` (for routes that take a body)
- `@Success` and `@Failure` for every documented status code
- `@Router` with method in brackets

## Body and response types

- Use Go model types directly: `{object} model.Rule`, `{array} model.Workflow`
- Wire shapes atropos-go exports (`RuleSync`, `RegisterResponse`, `StatusReport`,
  ...) are the bodies of the SDK-facing routes; reference them by the file's
  import alias, `{object} atroposdk.RuleSync`. The spec names them by the SDK's
  own `@name`, so they appear as `atropos.RuleSync`, next to `atropos.FaultRequest`
- Use `api.ErrorResponse` for all 4xx/5xx responses. The experiment and phase
  handlers additionally emit a machine-readable `code` (and code-specific
  fields) on top of `error` — see the error-envelope table in `CLAUDE.md`;
  the swag schema documents the common `error` field only
- For path/query params, use `@Param name in type required "description"`

## Streaming endpoints (SSE)

- `@Produce text/event-stream`
- Event types listed in the description text
- See run-events handler (when added) for the canonical example

## Regenerating the spec

Run `make openapi` from the repo root, then `make openapi-check` (it
regenerates and runs `git diff --exit-code -- docs/swagger.yaml
docs/swagger.json`, the same gate CI runs). swag resolves packages through the
same module setup as `go build`, so a local `go.work` needs a `go` line of at
least go.mod's.

Request bodies are emitted as a plain `$ref` to their schema. A field whose
type is a named schema carries its doc comment as a `description` beside the
`$ref` on the property, not on the shared component schema.

Commit `docs/swagger.{yaml,json}` together with handler edits. CI fails if the spec is stale.

## Known annotation gaps

These are places where the generated spec is approximate, mostly because the
underlying handler conflates error categories. They are tracked here so consumers
know not to over-trust the 4xx/5xx breakdown until the handler is fixed.

- `handleRegister` (`POST /sdk/register`) and `handleCreateRule` (`POST /rules`)
  collapse validation errors and infrastructure failures (DB unreachable, etc.)
  into a single 400. The proper split is 400 for validation, 500 for
  infrastructure. Until error classification is fixed in the handlers (separate
  follow-up), the spec's 4xx/5xx breakdown for these endpoints is approximate.
  Note: `handleRegister` is *not* a 409 candidate — `store.SDKRepo.Register` is
  an `INSERT ... ON CONFLICT DO UPDATE` upsert with no "duplicate" semantic to
  surface.
- `handleSSEEvents` (`GET /sdk/events`): swag applies an operation's single
  `@Produce` to every response, so the 400 and 500 error responses render as
  `text/event-stream` although the handler writes them as JSON
  (`api.ErrorResponse`) before any stream starts.
- `GET /cache/artifacts/{id}`: the same single-`@Produce` rule makes the JSON
  error bodies (`api.ErrorResponse` on 404 and 500) render as
  `application/x-ndjson`, the artifact's own type.
