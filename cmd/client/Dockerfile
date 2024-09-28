FROM golang:1.22 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/client ./cmd/client

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/client /client

ENTRYPOINT ["/client"]