FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/accumulators/percentile_accumulator ./cmd/steam_analyzer/accumulators/percentile_accumulator
COPY internal/system_protocol ./internal/system_protocol
COPY internal/mom ./internal/mom
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/accumulators/percentile_accumulator ./cmd/steam_analyzer/accumulators/percentile_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/accumulators/percentile_accumulator /accumulators/percentile_accumulator

ENTRYPOINT ["/accumulators/percentile_accumulator"]