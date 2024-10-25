FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY client ./client
COPY internal/client_protocol ./internal/client_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./client

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/client /client

ENTRYPOINT ["/client"]