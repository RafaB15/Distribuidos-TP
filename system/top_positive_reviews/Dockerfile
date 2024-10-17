FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/top_positive_reviews ./system/top_positive_reviews
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/top_positive_reviews ./system/top_positive_reviews

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/top_positive_reviews /top_positive_reviews

ENTRYPOINT ["/top_positive_reviews"]