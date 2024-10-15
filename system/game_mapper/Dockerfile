FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY system/game_mapper ./system/game_mapper
COPY middleware ./middleware
COPY internal/system_protocol ./internal/system_protocol
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/game_mapper ./system/game_mapper

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/game_mapper /game_mapper

ENTRYPOINT ["/game_mapper"]