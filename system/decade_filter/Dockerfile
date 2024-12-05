FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/decade_filter ./system/decade_filter
COPY middleware ./middleware
COPY persistence ./persistence
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/decade_filter ./system/decade_filter

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/decade_filter /decade_filter

ENTRYPOINT ["/decade_filter"]