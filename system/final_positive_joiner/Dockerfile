FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/final_positive_joiner ./system/final_positive_joiner
COPY middleware ./middleware
COPY internal/client_protocol ./internal/client_protocol
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/final_positive_joiner ./system/final_positive_joiner

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/final_positive_joiner /final_positive_joiner

ENTRYPOINT ["/final_positive_joiner"]