FROM golang:1.23.1 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/steam_analyzer/joiners/action_positive_review_joiner ./cmd/steam_analyzer/joiners/action_positive_review_joiner
COPY internal/system_protocol ./internal/system_protocol
COPY internal/mom ./internal/mom
COPY internal/utils ./internal/utils

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/joiners/action_positive_review_joiner ./cmd/steam_analyzer/joiners/action_positive_review_joiner

# Stage 2
FROM busybox:latest

COPY --from=builder /build/bin/joiners/action_positive_review_joiner /joiners/action_positive_review_joiner

ENTRYPOINT ["/joiners/action_positive_review_joiner"]