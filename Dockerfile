# Build stage
# Define a default value so it's not empty if the builder fails to provide it
ARG BUILDPLATFORM=linux/amd64

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /workspace

# The docker build context is the PARENT directory (set via skaffold.yaml
# `context: ..`), so we copy both sibling repos in. This matches the
# `replace git.ucsc.edu/microfaults/atropos-go => ../atropos-go` in go.mod
# and avoids the need for SSH/GOPRIVATE inside the container.
COPY atropos-go/ ./atropos-go/
COPY manteion-go/ ./manteion-go/

WORKDIR /workspace/manteion-go
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o /manteion ./cmd/manteion

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates \
    && adduser -D -u 1000 manteion

USER manteion
WORKDIR /home/manteion

COPY --from=builder /manteion /usr/local/bin/manteion

EXPOSE 8080

ENTRYPOINT ["manteion"]
