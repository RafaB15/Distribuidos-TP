default: build

all:

docker-compose-rabbit:
	docker compose -f docker-compose-dev.yaml up -d rabbitmq
.PHONY: docker-compose-rabbit

docker-image-system:
	docker build -f ./cmd/steam_analyzer/entrypoint/Dockerfile -t "entrypoint:latest" .
	docker build -f ./cmd/steam_analyzer/mappers/game_mapper/Dockerfile -t "game_mapper:latest" .
	docker build -f ./cmd/steam_analyzer/mappers/review_mapper/Dockerfile -t "review_mapper:latest" .
	docker build -f ./cmd/steam_analyzer/accumulators/os_accumulator/Dockerfile -t "os_accumulator:latest" .
	docker build -f ./cmd/steam_analyzer/accumulators/os_final_accumulator/Dockerfile -t "os_final_accumulator:latest" .
	docker build -f ./cmd/steam_analyzer/filters/english_filter/Dockerfile -t "english_filter:latest" .
	docker build -f ./cmd/steam_analyzer/filters/top_positive_reviews/Dockerfile -t "top_positive_reviews:latest" .
	docker build -f ./cmd/steam_analyzer/accumulators/english_reviews_accumulator/Dockerfile -t "english_reviews_accumulator:latest" .
	docker build -f ./cmd/steam_analyzer/accumulators/reviews_accumulator/Dockerfile -t "reviews_accumulator:latest" .
	docker build -f ./cmd/steam_analyzer/accumulators/top_ten_accumulator/Dockerfile -t "top_ten_accumulator:latest" .
	docker build -f ./cmd/steam_analyzer/filters/positive_reviews_filter/Dockerfile -t "positive_reviews_filter:latest" .
	docker build -f ./cmd/steam_analyzer/filters/decade_filter/Dockerfile -t "decade_filter:latest" .
	docker build -f ./cmd/steam_analyzer/joiners/action_review_joiner/Dockerfile -t "action_review_joiner:latest" .
	docker build -f ./cmd/steam_analyzer/joiners/indie_review_joiner/Dockerfile -t "indie_review_joiner:latest" .
	docker build -f ./cmd/steam_analyzer/writer/Dockerfile -t "writer:latest" .
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
