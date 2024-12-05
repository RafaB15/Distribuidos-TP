package main

import (
	u "distribuidos-tp/internal/utils"
	"flag"
	"fmt"
	"os"
)

func main() {
	configFile := flag.String("config", "config.json", "Configuration file")
	outputFile := flag.String("output", "docker-compose-dev.yaml", "Output Docker Compose file")
	flag.Parse()

	config, err := u.LoadConfig(*configFile)
	if err != nil {
		fmt.Printf("Error loading configuration file: %v\n", err)
		return
	}

	services := make(map[string]bool)

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
	services[serviceName] = true

	// Entrypoint service
	serviceName = "entrypoint"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: entrypoint:latest
    entrypoint: /entrypoint
    environment:
      - ACTION_REVIEW_JOINERS_AMOUNT=%d
      - REVIEW_ACCUMULATORS_AMOUNT=%d
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionReviewJoiner, config.ReviewsAccumulator)
	services[serviceName] = true

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

`, serviceName, serviceName, config.OSAccumulator, config.DecadeFilter, config.IndieReviewJoiner, config.ActionReviewJoiner)
	services[serviceName] = true

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
		services[serviceName] = true
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
	services[serviceName] = true

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
	services[serviceName] = true

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
	services[serviceName] = true

	// PercentileAccumulator service
	serviceName = "percentile_accumulator"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: percentile_accumulator:latest
    entrypoint: /percentile_accumulator
    environment:
      - NUM_PREVIOUS_ACCUMULATORS=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.ActionReviewAccumulator)
	services[serviceName] = true

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
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.IndieReviewJoiner)
		services[serviceName] = true
	}

	// DecadeFilter service
	for i := 1; i <= config.DecadeFilter; i++ {
		serviceName := fmt.Sprintf("decade_filter_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: decade_filter:latest
    entrypoint: /decade_filter
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
		services[serviceName] = true
	}

	//Action Review Joiner service
	for i := 1; i <= config.ActionReviewJoiner; i++ {
		serviceName := fmt.Sprintf("action_review_joiner_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_review_joiner:latest
    entrypoint: /action_review_joiner
    environment:
      - ID=%d
      - ENGLISH_FILTERS_AMOUNT=%d
      - ACTION_REVIEWS_ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishFilter, config.ActionReviewAccumulator)
		services[serviceName] = true
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
      - ACTION_REVIEW_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishReviewsAccumulator, config.ActionReviewJoiner)
		services[serviceName] = true
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
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.EnglishFilter)
		services[serviceName] = true
	}

	// NegativeReviewsFilter service

	serviceName = "negative_reviews_filter"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: negative_reviews_filter:latest
    entrypoint: /negative_reviews_filter
    environment:
      - ENGLISH_REVIEW_ACCUMULATORS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, config.EnglishReviewsAccumulator)
	services[serviceName] = true

	for i := 1; i <= config.ActionReviewAccumulator; i++ {
		serviceName := fmt.Sprintf("action_reviews_accumulator_%d", i)
		compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: action_reviews_accumulator:latest
    entrypoint: /action_reviews_accumulator
    environment:
      - ID=%d
      - ACTION_REVIEW_JOINERS_AMOUNT=%d
    depends_on:
      game_mapper:
        condition: service_started
      rabbitmq:
        condition: service_healthy
    networks:
      - distributed_network

`, serviceName, serviceName, i, config.ActionReviewJoiner)
		services[serviceName] = true
	}

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
		services[serviceName] = true
	}

	// Watchdog3 service
	serviceName = "watchdog3"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: watchdog:latest
    entrypoint: /watchdog
    environment:
      - WATCHDOG_HOST=3
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
`, serviceName, serviceName)
	for dep := range services {
		compose += fmt.Sprintf("      %s:\n        condition: service_started\n", dep)
	}
	compose += `    networks:
      - distributed_network

`
	services[serviceName] = true

	// Watchdog1 service
	serviceName = "watchdog1"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: watchdog:latest
    entrypoint: /watchdog
    environment:
      - WATCHDOG_HOST=1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      watchdog3:
        condition: service_started
    networks:
      - distributed_network

`, serviceName, serviceName)
	services[serviceName] = true

	// Watchdog2 service
	serviceName = "watchdog2"
	compose += fmt.Sprintf(`  %s:
    container_name: %s
    image: watchdog:latest
    entrypoint: /watchdog
    environment:
      - WATCHDOG_HOST=2
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      watchdog3:
        condition: service_started
    networks:
      - distributed_network

`, serviceName, serviceName)
	services[serviceName] = true

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
