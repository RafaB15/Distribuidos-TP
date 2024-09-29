FROM golang:1.22 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steamanalizer/entrypoint ./cmd/steamanalizer/entrypoint
COPY internal/clientprotocol ./internal/clientprotocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/entrypoint ./cmd/steamanalizer/entrypoint

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/entrypoint /entrypoint

ENTRYPOINT ["/entrypoint"]