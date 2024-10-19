package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	Rabbitmq                   int `json:"rabbitmq"`
	Entrypoint                 int `json:"entrypoint"`
	GameMapper                 int `json:"game_mapper"`
	OSAccumulator              int `json:"os_accumulator"`
	OSFinalAccumulator         int `json:"os_final_accumulator"`
	TopTenAccumulator          int `json:"top_ten_accumulator"`
	TopPositiveReviews         int `json:"top_positive_reviews"`
	PercentileAccumulator      int `json:"percentile_accumulator"`
	ReviewMapper               int `json:"review_mapper"`
	ReviewsAccumulator         int `json:"reviews_accumulator"`
	DecadeFilter               int `json:"decade_filter"`
	EnglishFilter              int `json:"english_filter"`
	EnglishReviewsAccumulator  int `json:"english_reviews_accumulator"`
	PositiveReviewsFilter      int `json:"positive_reviews_filter"`
	ActionPositiveReviewJoiner int `json:"action_positive_review_joiner"`
	ActionNegativeReviewJoiner int `json:"action_negative_review_joiner"`
	IndieReviewJoiner          int `json:"indie_review_joiner"`
	Writer                     int `json:"writer"`
}

func main() {
	configFile := flag.String("config", "config.json", "Configuration file")
	outputFile := flag.String("output", "docker-compose-dev.yaml", "Output Docker Compose file")
	flag.Parse()

	config := Config{}
	data, err := os.ReadFile(*configFile)
	if err != nil {
		fmt.Printf("Error reading configuration file: %v\n", err)
		return
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Printf("Error parsing configuration file: %v\n", err)
		return
	}

	compose := `services:
`

	// RabbitMQ service
	serviceName := "rabbitmq"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: rabbitmq:4-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    healthcheck:
      test: [ "CMD", "rabbitmqctl", "status" ]
      interval: 15s
      timeout: 10s
      retries: 5
    networks:
      - distributed_network

`, serviceName, serviceName)

	// Entrypoint service
	serviceName = "entrypoint"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: entrypoint:latest
    entrypoint: /entrypoint
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName)

	// GameMapper service
	serviceName = "game_mapper"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: game_mapper:latest
    entrypoint: /mappers/game_mapper
    environment:
      - OS_ACCUMULATORS_AMOUNT=%d
      - DECADE_FILTER_AMOUNT=%d
      - INDIE_REVIEW_JOINERS_AMOUNT=%d
      - ACTION_REVIEW_JOINERS_AMOUNT=%d
    depends_on:
      entrypoint:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.OSAccumulator, config.DecadeFilter, config.IndieReviewJoiner, config.ActionPositiveReviewJoiner)

	// OSAccumulator service
	for i := 1; i <= config.OSAccumulator; i++ {
		serviceName := fmt.Sprintf("os_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: os_accumulator:latest
    entrypoint: /accumulators/os_accumulator
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName)
	}

	// OSFinalAccumulator service
	serviceName = "os_final_accumulator"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: os_final_accumulator:latest
    entrypoint: /accumulators/os_final_accumulator
    environment:
      - NUM_PREVIOUS_OS_ACCUMULATORS=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.OSAccumulator)

	// TopTenAccumulator service
	serviceName = "top_ten_accumulator"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: top_ten_accumulator:latest
    entrypoint: /top_ten_accumulator
    environment:
      - DECADE_FILTERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.DecadeFilter)

	// TopPositiveReviews service
	serviceName = "top_positive_reviews"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: top_positive_reviews:latest
    entrypoint: /filters/top_positive_reviews
    environment:
      - INDIE_REVIEW_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.IndieReviewJoiner)

	// PercentileAccumulator service
	serviceName = "percentile_accumulator"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: percentile_accumulator:latest
    entrypoint: /accumulators/percentile_accumulator
    environment:
      - ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT=%d
      - NUM_PREVIOUS_ACCUMULATORS=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionNegativeReviewJoiner, config.ReviewsAccumulator)

	// ReviewMapper service
	for i := 1; i <= config.ReviewMapper; i++ {
		serviceName := fmt.Sprintf("review_mapper_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: review_mapper:latest
    entrypoint: /mappers/review_mapper
    environment:
      - ID=%d
      - ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.ReviewsAccumulator)
	}

	// ReviewsAccumulator service
	for i := 1; i <= config.ReviewsAccumulator; i++ {
		serviceName := fmt.Sprintf("reviews_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: reviews_accumulator:latest
    entrypoint: /accumulators/reviews_accumulator
    environment:
      - ID=%d
      - MAPPERS_AMOUNT=%d
      - INDIE_REVIEW_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.ReviewMapper, config.IndieReviewJoiner)
	}

	// DecadeFilter service
	for i := 1; i <= config.DecadeFilter; i++ {
		serviceName := fmt.Sprintf("decade_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: decade_filter:new_version
    entrypoint: /decade_filter
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName)
	}

	// EnglishFilter service
	for i := 1; i <= config.EnglishFilter; i++ {
		serviceName := fmt.Sprintf("english_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: english_filter:latest
    entrypoint: /filters/english_filter
    environment:
      - ID=%d
      - ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishReviewsAccumulator)
	}

	// EnglishReviewsAccumulator service
	for i := 1; i <= config.EnglishReviewsAccumulator; i++ {
		serviceName := fmt.Sprintf("english_reviews_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: english_reviews_accumulator:latest
    entrypoint: /accumulators/english_reviews_accumulator
    environment:
      - ID=%d
      - FILTERS_AMOUNT=%d
      - POSITIVE_REVIEWS_FILTER_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishFilter, config.PositiveReviewsFilter)
	}

	// PositiveReviewsFilter service
	for i := 1; i <= config.PositiveReviewsFilter; i++ {
		serviceName := fmt.Sprintf("positive_reviews_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: positive_reviews_filter:latest
    entrypoint: /filters/positive_reviews_filter
    environment:
      - ACTION_POSITIVE_REVIEWS_JOINERS_AMOUNT=%d
      - ENGLISH_REVIEW_ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionPositiveReviewJoiner, config.EnglishReviewsAccumulator)
	}

	// ActionPositiveReviewJoiner service
	for i := 1; i <= config.ActionPositiveReviewJoiner; i++ {
		serviceName := fmt.Sprintf("action_positive_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_positive_review_joiner:latest
    entrypoint: /joiners/action_positive_review_joiner
    environment:
      - ID=%d
      - POSITIVE_REVIEWS_FILTERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.PositiveReviewsFilter)
	}

	// ActionPositiveReviewJoiner service
	for i := 1; i <= config.ActionNegativeReviewJoiner; i++ {
		serviceName := fmt.Sprintf("action_negative_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_negative_review_joiner:latest
    entrypoint: /joiners/action_negative_review_joiner
    environment:
      - ID=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i)
	}

	// IndieReviewJoiner service
	for i := 1; i <= config.IndieReviewJoiner; i++ {
		serviceName := fmt.Sprintf("indie_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: indie_review_joiner:latest
    entrypoint: /joiners/indie_review_joiner
    environment:
      - ID=%d
      - REVIEWS_ACCUMULATOR_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.ReviewsAccumulator)
	}

	// Writer service
	serviceName = "writer"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: writer:latest
    entrypoint: /writer
    environment:
      - ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT=%d
      - ACTION_POSITIVE_REVIEWS_JOINERS_AMOUNT=%d
    depends_on:
      entrypoint:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionNegativeReviewJoiner, config.ActionPositiveReviewJoiner)

	compose += `networks:
  distributed_network:
    name: distributed_network
    ipam:
      driver: default
      config:
        - subnet: 172.24.125.0/24
`

	err = os.WriteFile(*outputFile, []byte(compose), 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		return
	}
}
