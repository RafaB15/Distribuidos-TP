package main

import (
	u "distribuidos-tp/internal/utils"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                          = "ID"
	FiltersAmountEnvironmentVariableName               = "FILTERS_AMOUNT"
	PositiveReviewsFilterAmountEnvironmentVariableName = "POSITIVE_REVIEWS_FILTER_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	filtersAmount, err := u.GetEnvInt(FiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	positiveReviewsFilterAmount, err := u.GetEnvInt(PositiveReviewsFilterAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}
}
