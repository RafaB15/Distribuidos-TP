FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/indie_review_joiner ./system/indie_review_joiner
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/indie_review_joiner ./system/indie_review_joiner

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/indie_review_joiner /indie_review_joiner

ENTRYPOINT ["/indie_review_joiner"]