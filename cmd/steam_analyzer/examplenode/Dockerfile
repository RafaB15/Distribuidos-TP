FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/examplenode ./cmd/steam_analyzer/examplenode
COPY internal/client_protocol ./internal/client_protocol
COPY internal/utils ./internal/utils
COPY internal/mom ./internal/mom

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/examplenode ./cmd/steam_analyzer/examplenode

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/examplenode /examplenode

ENTRYPOINT ["/examplenode"]