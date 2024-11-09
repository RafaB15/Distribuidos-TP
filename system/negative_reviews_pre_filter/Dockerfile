FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/negative_reviews_pre_filter ./system/negative_reviews_pre_filter
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/negative_reviews_pre_filter ./system/negative_reviews_pre_filter

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/negative_reviews_pre_filter /negative_reviews_pre_filter

ENTRYPOINT ["/negative_reviews_pre_filter"]