package utils

import (
	"encoding/json"
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
	ActionReviewJoiner           int `json:"action_review_joiner"`
	ActionReviewAccumulator      int `json:"action_review_accumulator"`
	EnglishFilter                int `json:"english_filter"`
	EnglishReviewsAccumulator    int `json:"english_reviews_accumulator"`
	NegativeReviewsFilter        int `json:"negative_reviews_filter"`
	ActionPercentileReviewJoiner int `json:"action_percentile_review_joiner"`
	ActionEnglishReviewJoiner    int `json:"action_english_review_joiner"`
	IndieReviewJoiner            int `json:"indie_review_joiner"`
	FinalEnglishJoiner           int `json:"final_english_joiner"`
	FinalPercentileJoiner        int `json:"final_percentile_joiner"`
}

func LoadConfig(filename string) (Config, error) {

	config := Config{}

	bytes, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(bytes, &config)
	return config, err
}
