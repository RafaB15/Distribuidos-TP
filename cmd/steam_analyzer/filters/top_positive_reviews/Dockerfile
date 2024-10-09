FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/filters/top_positive_reviews ./cmd/steam_analyzer/filters/top_positive_reviews 
COPY internal/mom ./internal/mom
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filters/top_positive_reviews ./cmd/steam_analyzer/filters/top_positive_reviews

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/filters/top_positive_reviews /filters/top_positive_reviews

ENTRYPOINT ["/filters/top_positive_reviews"]