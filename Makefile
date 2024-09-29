default: build

all:

docker-image:
	docker build -f ./cmd/client/Dockerfile -t "client:latest" .
	docker build -f ./cmd/steamanalizer/entrypoint/Dockerfile -t "entrypoint:latest" .
	docker build -f ./cmd/steamanalizer/examplenode/Dockerfile -t "examplenode:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs