# OpenAPI spec generation via swaggo/swag v2.
#
# Usage:
#   make openapi        Regenerate docs/swagger.{yaml,json} from handler annotations.
#   make openapi-check  Regenerate then assert the result is unchanged (CI gate).
#   make openapi-local  `make openapi` on a host whose Go is newer than swag can parse
#                       (see below). Check by hand: make openapi-local && git diff
#                       --exit-code -- docs/swagger.yaml docs/swagger.json
#
# Requirements:
#   - swag v2 binary on PATH; install with:
#       go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5
#     (v2.0.0 has not been released yet; rc5 is the latest pre-release.)
#   - python3 with pyyaml for the post-processor
#       Linux:  apt-get install python3-yaml  (or your distro's equivalent)
#       macOS:  brew install pyyaml           (or pip install --user --break-system-packages pyyaml)
#     The post-processor strips empty externalDocs blocks that swag v2 emits
#     unconditionally — remove this hop once swag publishes a release that
#     omits empty stubs.
#
# Notes:
#   - --generalInfo takes a bare filename resolved against the FIRST --dir entry.
#     With --dir cmd/manteion,..., the file resolves to cmd/manteion/main.go.
#     Passing the full path doubles the prefix and fails — this is a known swag v2 quirk.
#   - --parseInternal is required because the API package lives under internal/.
#   - --outputTypes yaml,json commits both for human review (yaml) and tool consumption (json).

.PHONY: openapi openapi-check openapi-local test build verify integration e2e

openapi:
	swag init \
		--v3.1 \
		--generalInfo main.go \
		--dir cmd/manteion,internal/api,internal/model,internal/opaharness \
		--output docs \
		--outputTypes yaml,json \
		--parseDependency \
		--parseInternal
	@python3 scripts/strip-empty-externaldocs.py docs/swagger.yaml docs/swagger.json

openapi-check: openapi
	@git diff --exit-code -- docs/swagger.yaml docs/swagger.json || \
		(echo "ERROR: swagger spec is stale. Run 'make openapi' and commit." && exit 1)

# swag rc5 cannot parse a newer Go's stdlib (Go 1.27: "method must have no type
# parameters" in math/rand/v2), and GOTOOLCHAIN=go<go.mod version> alone fails
# against a local go.work that requires the newer Go ("go.work requires go >=
# ..."). openapi-local runs `make openapi` under the go.mod toolchain through a
# copy of go.work (same use/replace lines, beside the original so relative paths
# still resolve) whose go line is lowered to the go.mod version; the copy is
# removed afterwards. Without a go.work, GOTOOLCHAIN alone suffices.
OPENAPI_GO = $(shell sed -n 's/^go //p' go.mod)

openapi-local:
	@if [ -f go.work ]; then \
		trap 'rm -f .openapi.go.work .openapi.go.work.sum' EXIT; \
		cp go.work .openapi.go.work && \
		go work edit -go=$(OPENAPI_GO) -toolchain=none .openapi.go.work && \
		GOTOOLCHAIN=go$(OPENAPI_GO) GOWORK=$(CURDIR)/.openapi.go.work $(MAKE) --no-print-directory openapi; \
	else \
		GOTOOLCHAIN=go$(OPENAPI_GO) $(MAKE) --no-print-directory openapi; \
	fi

test:
	go test ./...

build:
	go build ./...

verify: build
	@echo "=== gofmt ==="
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "These files need gofmt:"; echo "$$unformatted"; exit 1; \
	fi
	@echo "=== go vet ==="
	go vet ./...
	@echo "=== go test ==="
	go test ./...

# -p 1 serializes package execution: the DB-gated suites (store,
# orchestrator) share one Postgres, and testutil.TestDB's cleanup truncates
# every table — parallel packages would wipe each other's fixtures mid-test.
integration:
	MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test -count=1 -timeout 300s -p 1 ./...

e2e:
	docker-compose up -d
	MANTEION_E2E=1 \
	MANTEION_INTEGRATION=1 \
	MANTEION_DATABASE_URL=postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable \
		go test -count=1 -timeout 300s -p 1 ./...
