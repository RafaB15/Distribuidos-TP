FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/top_ten_accumulator ./system/top_ten_accumulator
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/top_ten_accumulator ./system/top_ten_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/top_ten_accumulator /top_ten_accumulator

ENTRYPOINT ["/top_ten_accumulator"]