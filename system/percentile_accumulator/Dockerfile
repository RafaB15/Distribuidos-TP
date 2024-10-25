FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/percentile_accumulator ./system/percentile_accumulator
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/percentile_accumulator ./system/percentile_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/percentile_accumulator /percentile_accumulator

ENTRYPOINT ["/percentile_accumulator"]