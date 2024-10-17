package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/indie_review_joiner/logic"
	m "distribuidos-tp/system/indie_review_joiner/middleware"

	"github.com/op/go-logging"
)

const (
	ReviewsAccumulatorAmountEnvironmentVariableName = "REVIEWS_ACCUMULATOR_AMOUNT"
	IdEnvironmentVariableName                       = "ID"
)

var log = logging.MustGetLogger("log")

func main() {

	accumulatorsAmount, err := u.GetEnvInt(ReviewsAccumulatorAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	reviewJoiner := l.NewIndieReviewJoiner(middleware.ReceiveMsg, m.HandleGameReviewMetrics, m.HandleGameNames, middleware.SendMetrics, middleware.SendEof)
	reviewJoiner.Run(accumulatorsAmount)
}
