FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/os_final_accumulator ./system/os_final_accumulator
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils
COPY persistence ./persistence

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/os_final_accumulator ./system/os_final_accumulator

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/os_final_accumulator /os_final_accumulator

ENTRYPOINT ["/os_final_accumulator"]