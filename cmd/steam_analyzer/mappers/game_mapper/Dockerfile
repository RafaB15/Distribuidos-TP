FROM golang:1.23 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY internal/client_protocol ./internal/client_protocol
COPY internal/utils ./internal/utils
COPY internal/mom ./internal/mom

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/mappers/game_mapper ./cmd/steam_analyzer/mappers/game_mapper

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/mappers/game_mapper /mappers/game_mapper

ENTRYPOINT ["/mappers/game_mapper"]