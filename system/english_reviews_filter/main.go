package main

import (
	u "distribuidos-tp/internal/utils"

	l "distribuidos-tp/system/english_reviews_filter/logic"
	m "distribuidos-tp/system/english_reviews_filter/middleware"

	"github.com/op/go-logging"
)

const (
	AccumulatorsAmountEnvironmentVariableName = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                 = "ID"
)

var log = logging.MustGetLogger("log")

func main() {
	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	englishReviewsFilter := l.NewEnglishReviewsFilter(
		middleware.ReceiveGameReviews,
		middleware.SendEnglishReviews,
		middleware.SendEndOfFiles,
	)

	englishReviewsFilter.Run(accumulatorsAmount)
}
