# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /manteion ./cmd/manteion

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates \
    && adduser -D -u 1000 manteion

USER manteion
WORKDIR /home/manteion

COPY --from=builder /manteion /usr/local/bin/manteion

EXPOSE 8080

ENTRYPOINT ["manteion"]
