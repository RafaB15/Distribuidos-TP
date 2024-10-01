default: build

all:

docker-compose-rabbit:
	docker compose -f docker-compose-dev.yaml up -d rabbitmq
.PHONY: docker-compose-rabbit

docker-image-system:
	docker build -f ./cmd/steam_analyzer/entrypoint/Dockerfile -t "entrypoint:latest" .
	docker build -f ./cmd/steam_analyzer/examplenode/Dockerfile -t "examplenode:latest" .
.PHONY: docker-image-system

docker-compose-up: docker-image-system
	docker compose -f docker-compose-dev.yaml up -d
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-image-clients:
	docker build -f ./cmd/client/Dockerfile -t "client:latest" .
.PHONY: docker-image-client 

docker-client: docker-image-clients
	docker run -d --name client -v ./cmd/client/client_data:/client_data --network distributed_network client:latest
.PHONY: docker-client

docker-client-remove:
	docker stop client || true
	docker rm client || true
.PHONY: docker-client-remove
