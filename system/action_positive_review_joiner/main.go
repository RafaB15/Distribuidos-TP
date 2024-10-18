package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/action_positive_review_joiner/logic"
	m "distribuidos-tp/system/action_positive_review_joiner/middleware"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                           = "ID"                              //al middleware
	PositiveReviewsFiltersAmountEnvironmentVariableName = "POSITIVE_REVIEWS_FILTERS_AMOUNT" //al run

)

var log = logging.MustGetLogger("log")

func main() {
	positiveReviewsFiltersAmount, err := u.GetEnvInt(PositiveReviewsFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	positiveActionReviewJoiner := l.NewActionPositiveReviewJoiner(middleware.ReceiveMsg, m.HandleGameReviewMetrics, m.HandleGameNames, middleware.SendMetrics, middleware.SendEof)
	positiveActionReviewJoiner.Run(positiveReviewsFiltersAmount)
}
