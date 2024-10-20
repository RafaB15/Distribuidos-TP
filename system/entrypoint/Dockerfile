FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/entrpoint ./system/entrpoint
COPY middleware ./middleware
COPY internal/client_protocol ./internal/client_protocol
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/entrpoint ./system/entrpoint

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/entrpoint /entrpoint

ENTRYPOINT ["/entrpoint"]