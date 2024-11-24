FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/action_review_joiner ./system/action_review_joiner
COPY middleware ./middleware
COPY persistence ./persistence
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/action_review_joiner ./system/action_review_joiner

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/action_review_joiner /action_review_joiner

ENTRYPOINT ["/action_review_joiner"]