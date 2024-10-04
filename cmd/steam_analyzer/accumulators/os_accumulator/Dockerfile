FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/accumulators/os_accumulator ./cmd/steam_analyzer/accumulators/os_accumulator
COPY internal/mom ./internal/mom
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/accumulators/os_accumulator ./cmd/steam_analyzer/accumulators/os_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/accumulators/os_accumulator /accumulators/os_accumulator

ENTRYPOINT ["/accumulators/os_accumulator"]