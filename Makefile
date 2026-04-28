# OpenAPI spec generation via swaggo/swag v2.
#
# Usage:
#   make openapi        Regenerate docs/swagger.{yaml,json} from handler annotations.
#   make openapi-check  Regenerate then assert the result is unchanged (CI gate).
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

.PHONY: openapi openapi-check test build

openapi:
	swag init \
		--v3.1 \
		--generalInfo main.go \
		--dir cmd/manteion,internal/api,internal/model \
		--output docs \
		--outputTypes yaml,json \
		--parseDependency \
		--parseInternal
	@python3 scripts/strip-empty-externaldocs.py docs/swagger.yaml docs/swagger.json

openapi-check: openapi
	@git diff --exit-code -- docs/swagger.yaml docs/swagger.json || \
		(echo "ERROR: swagger spec is stale. Run 'make openapi' and commit." && exit 1)

test:
	go test ./...

build:
	go build ./...
