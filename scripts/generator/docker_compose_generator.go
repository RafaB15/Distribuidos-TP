package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	Rabbitmq                     int `json:"rabbitmq"`
	Entrypoint                   int `json:"entrypoint"`
	GameMapper                   int `json:"game_mapper"`
	OSAccumulator                int `json:"os_accumulator"`
	OSFinalAccumulator           int `json:"os_final_accumulator"`
	TopTenAccumulator            int `json:"top_ten_accumulator"`
	TopPositiveReviews           int `json:"top_positive_reviews"`
	PercentileAccumulator        int `json:"percentile_accumulator"`
	ReviewsAccumulator           int `json:"reviews_accumulator"`
	DecadeFilter                 int `json:"decade_filter"`
	NegativeReviewsPreFilter     int `json:"negative_reviews_pre_filter"`
	EnglishFilter                int `json:"english_filter"`
	EnglishReviewsAccumulator    int `json:"english_reviews_accumulator"`
	NegativeReviewsFilter        int `json:"negative_reviews_filter"`
	ActionEnglishReviewJoiner    int `json:"action_english_review_joiner"`
	ActionPercentileReviewJoiner int `json:"action_percentile_review_joiner"`
	IndieReviewJoiner            int `json:"indie_review_joiner"`
	FinalEnglishJoiner           int `json:"final_english_joiner"`
	FinalPercentileJoiner        int `json:"final_percentile_joiner"`
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
    environment:
      - NEGATIVE_REVIEWS_PRE_FILTERS_AMOUNT=%d
      - REVIEW_ACCUMULATORS_AMOUNT=%d
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.NegativeReviewsPreFilter, config.ReviewsAccumulator)

	// GameMapper service
	serviceName = "game_mapper"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: game_mapper:latest
    entrypoint: /game_mapper
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

`, serviceName, serviceName, config.OSAccumulator, config.DecadeFilter, config.IndieReviewJoiner, config.ActionEnglishReviewJoiner)

	// OSAccumulator service
	for i := 1; i <= config.OSAccumulator; i++ {
		serviceName := fmt.Sprintf("os_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: os_accumulator:latest
    entrypoint: /os_accumulator
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

	// OSFinalAccumulator service
	serviceName = "os_final_accumulator"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: os_final_accumulator:latest
    entrypoint: /os_final_accumulator
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
    entrypoint: /top_positive_reviews
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
    entrypoint: /percentile_accumulator
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

`, serviceName, serviceName, config.ActionPercentileReviewJoiner, config.ReviewsAccumulator)

	// ReviewsAccumulator service
	for i := 1; i <= config.ReviewsAccumulator; i++ {
		serviceName := fmt.Sprintf("reviews_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: reviews_accumulator:latest
    entrypoint: /reviews_accumulator
    environment:
      - ID=%d
      - INDIE_REVIEW_JOINERS_AMOUNT=%d
      - NEGATIVE_REVIEWS_PRE_FILTERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.IndieReviewJoiner, config.NegativeReviewsPreFilter)
	}

	// DecadeFilter service
	for i := 1; i <= config.DecadeFilter; i++ {
		serviceName := fmt.Sprintf("decade_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: decade_filter:latest
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

	//Negative Reviews Pre Filter service
	for i := 1; i <= config.NegativeReviewsPreFilter; i++ {
		serviceName := fmt.Sprintf("negative_reviews_pre_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: negative_reviews_pre_filter:latest
    entrypoint: /negative_reviews_pre_filter
    environment:
      - ID=%d
      - ENGLISH_FILTERS_AMOUNT=%d
      - ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishFilter, config.ReviewsAccumulator)
	}

	// EnglishFilter service
	for i := 1; i <= config.EnglishFilter; i++ {
		serviceName := fmt.Sprintf("english_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: english_reviews_filter:latest
    entrypoint: /english_reviews_filter
    environment:
      - ID=%d
      - ACCUMULATORS_AMOUNT=%d
      - NEGATIVE_REVIEWS_PRE_FILTERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishReviewsAccumulator, config.NegativeReviewsPreFilter)
	}

	// EnglishReviewsAccumulator service
	for i := 1; i <= config.EnglishReviewsAccumulator; i++ {
		serviceName := fmt.Sprintf("english_reviews_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: english_reviews_accumulator:latest
    entrypoint: /english_reviews_accumulator
    environment:
      - ID=%d
      - FILTERS_AMOUNT=%d
      - NEGATIVE_REVIEWS_FILTER_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishFilter, config.NegativeReviewsFilter)
	}

	// NegativeReviewsFilter service
	for i := 1; i <= config.NegativeReviewsFilter; i++ {
		serviceName := fmt.Sprintf("negative_reviews_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: negative_reviews_filter:latest
    entrypoint: /negative_reviews_filter
    environment:
      - ACTION_ENGLISH_REVIEWS_JOINERS_AMOUNT=%d
      - ENGLISH_REVIEW_ACCUMULATORS_AMOUNT=%d
      - ID=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionEnglishReviewJoiner, config.EnglishReviewsAccumulator, i)
	}

	// ActionEnglishReviewJoiner service
	for i := 1; i <= config.ActionEnglishReviewJoiner; i++ {
		serviceName := fmt.Sprintf("action_english_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_english_review_joiner:latest
    entrypoint: /action_english_review_joiner
    environment:
      - ID=%d
      - NEGATIVE_REVIEWS_FILTERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.NegativeReviewsFilter)
	}

	serviceName = "final_english_joiner"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: final_english_joiner:latest
    entrypoint: /final_english_joiner
    environment:
      - ACTION_ENGLISH_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionEnglishReviewJoiner)

	// ActionPercentileReviewJoiner service
	for i := 1; i <= config.ActionPercentileReviewJoiner; i++ {
		serviceName := fmt.Sprintf("action_percentile_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_percentile_review_joiner:latest
    entrypoint: /action_percentile_review_joiner
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

	serviceName = "final_percentile_joiner"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: final_percentile_joiner:latest
    entrypoint: /final_percentile_joiner
    environment:
      - ACTION_PERCENTILE_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionPercentileReviewJoiner)

	// IndieReviewJoiner service
	for i := 1; i <= config.IndieReviewJoiner; i++ {
		serviceName := fmt.Sprintf("indie_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: indie_review_joiner:latest
    entrypoint: /indie_review_joiner
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
