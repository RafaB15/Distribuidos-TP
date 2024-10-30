FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/positive_reviews_filter ./system/positive_reviews_filter
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/positive_reviews_filter ./system/positive_reviews_filter

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/positive_reviews_filter /positive_reviews_filter

ENTRYPOINT ["/positive_reviews_filter"]