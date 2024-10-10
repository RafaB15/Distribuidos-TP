FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/accumulators/negative_review_accumulator ./cmd/steam_analyzer/accumulators/negative_review_accumulator
COPY internal/system_protocol ./internal/system_protocol
COPY internal/mom ./internal/mom
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/accumulators/negative_review_accumulator ./cmd/steam_analyzer/accumulators/negative_review_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/accumulators/negative_review_accumulator /accumulators/negative_review_accumulator

ENTRYPOINT ["/accumulators/negative_review_accumulator"]