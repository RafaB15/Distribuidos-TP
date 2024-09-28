FROM golang:1.22 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/entrypoint ./cmd/steamanalizer/entrypoint

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/entrypoint /entrypoint

ENTRYPOINT ["/entrypoint"]