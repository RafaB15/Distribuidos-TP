FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/watchdog ./system/watchdog

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/watchdog ./system/watchdog

# Stage 2
FROM busybox:latest

# Install Docker
RUN mkdir -p /etc/ssl/certs/ /usr/local/bin/ && \
    wget https://download.docker.com/linux/static/stable/x86_64/docker-20.10.7.tgz && \
    tar --extract --file docker-20.10.7.tgz --strip-components 1 --directory /usr/local/bin/ && \
    rm docker-20.10.7.tgz

COPY --from=builder /build/bin/watchdog /watchdog

ENTRYPOINT ["/watchdog"]