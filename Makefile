default: build

all:

docker-compose-rabbit:
	docker compose -f docker-compose-dev.yaml up -d rabbitmq
.PHONY: docker-compose-rabbit

docker-image-system:
	docker build -f ./system/top_positive_reviews/Dockerfile -t "top_positive_reviews:new_version" .
	docker build -f ./system/top_ten_accumulator/Dockerfile -t "top_ten_accumulator:new_version" .	
	docker build -f ./system/percentile_accumulator/Dockerfile -t "percentile_accumulator:new_version" .
	docker build -f ./system/decade_filter/Dockerfile -t "decade_filter:new_version" .
	docker build -f ./cmd/steam_analyzer/writer/Dockerfile -t "writer:latest" .
	docker build -f ./system/game_mapper/Dockerfile -t "game_mapper:new_version" .
	docker build -f ./system/os_accumulator/Dockerfile -t "os_accumulator:new_version" .
	docker build -f ./system/os_final_accumulator/Dockerfile -t "os_final_accumulator:new_version" .
	docker build -f ./system/reviews_accumulator/Dockerfile -t "reviews_accumulator:new_version" .
	docker build -f ./system/indie_review_joiner/Dockerfile -t "indie_review_joiner:new_version" .
	docker build -f ./system/action_positive_review_joiner/Dockerfile -t "action_positive_review_joiner:new_version" .
	docker build -f ./system/positive_reviews_filter/Dockerfile -t "positive_reviews_filter:new_version" .
	docker build -f ./system/action_negative_review_joiner/Dockerfile -t "action_negative_review_joiner:new_version" .
	docker build -f ./system/english_reviews_accumulator/Dockerfile -t "english_reviews_accumulator:new_version" .
	docker build -f ./system/english_reviews_filter/Dockerfile -t "english_reviews_filter:new_version" .
	docker build -f ./system/review_mapper/Dockerfile -t "review_mapper:new_version" .
	docker build -f ./system/entrypoint/Dockerfile -t "entrypoint:new_version" .

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
	docker run -d --name client \
		-v ./cmd/client/client_data:/client_data \
		--network distributed_network \
		-e GAME_FILE_PATH=./client_data/games_90k.csv \
		-e REVIEW_FILE_PATH=./client_data/steam_reviews_500k.csv \
		client:latest
.PHONY: docker-client

docker-client-remove:
	docker stop client || true
	docker rm client || true
.PHONY: docker-client-remove

all-down: docker-compose-down docker-client-remove

generate_compose:
	./generar-compose.sh config.json docker-compose-dev.yaml
.PHONY: docker-client-remove