FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/os_accumulator ./system/os_accumulator
COPY middleware ./middleware
COPY persistence ./persistence
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/os_accumulator ./system/os_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/os_accumulator /os_accumulator

ENTRYPOINT ["/os_accumulator"]