default: build

all:

docker-compose-rabbit:
	docker compose -f docker-compose-dev.yaml up -d rabbitmq
.PHONY: docker-compose-rabbit

docker-image-system:
	docker build -f ./system/top_positive_reviews/Dockerfile -t "top_positive_reviews:latest" .
	docker build -f ./system/top_ten_accumulator/Dockerfile -t "top_ten_accumulator:latest" .	
	docker build -f ./system/percentile_accumulator/Dockerfile -t "percentile_accumulator:latest" .
	docker build -f ./system/decade_filter/Dockerfile -t "decade_filter:latest" .
	docker build -f ./system/game_mapper/Dockerfile -t "game_mapper:latest" .
	docker build -f ./system/os_accumulator/Dockerfile -t "os_accumulator:latest" .
	docker build -f ./system/os_final_accumulator/Dockerfile -t "os_final_accumulator:latest" .
	docker build -f ./system/reviews_accumulator/Dockerfile -t "reviews_accumulator:latest" .
	docker build -f ./system/indie_review_joiner/Dockerfile -t "indie_review_joiner:latest" .
	# docker build -f ./system/action_english_review_joiner/Dockerfile -t "action_english_review_joiner:latest" .
	docker build -f ./system/negative_reviews_filter/Dockerfile -t "negative_reviews_filter:latest" .
	# docker build -f ./system/action_percentile_review_joiner/Dockerfile -t "action_percentile_review_joiner:latest" .
	docker build -f ./system/english_reviews_accumulator/Dockerfile -t "english_reviews_accumulator:latest" .
	docker build -f ./system/english_reviews_filter/Dockerfile -t "english_reviews_filter:latest" .
	docker build -f ./system/entrypoint/Dockerfile -t "entrypoint:latest" .
	# docker build -f ./system/final_english_joiner/Dockerfile -t "final_english_joiner:latest" .
	# docker build -f ./system/final_percentile_joiner/Dockerfile -t "final_percentile_joiner:latest" .
	docker build -f ./system/action_review_joiner/Dockerfile -t "action_review_joiner:latest" .
	docker build -f ./system/action_reviews_accumulator/Dockerfile -t "action_reviews_accumulator:latest" .
	docker build -f ./system/watchdog/Dockerfile -t "watchdog:latest" .
.PHONY: docker-image-system

docker-compose-up: docker-image-system
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-image-clients:
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image-client 

docker-client: docker-image-clients
	docker run -d --name client \
		-v ./client/client_data:/client_data \
		--network distributed_network \
		-e GAME_FILE_PATH=./client_data/games_90k.csv \
		-e REVIEW_FILE_PATH=./client_data/steam_reviews_1M.csv \
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

docker-compose-clients:
	docker-compose -f docker-compose-clients.yaml up -d
.PHONY: docker-compose-clients

docker-compose-clients-down:
	docker-compose -f docker-compose-clients.yaml down
.PHONY: docker-compose-clients-down

docker-compose-clients-logs:
	docker-compose -f docker-compose-clients.yaml logs -f
.PHONY: docker-compose-clients-logs