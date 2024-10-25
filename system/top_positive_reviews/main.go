package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/top_positive_reviews/logic"
	m "distribuidos-tp/system/top_positive_reviews/middleware"

	"github.com/op/go-logging"
)

const (
	IndieReviewJoinersAmountEnvironmentVariableName = "INDIE_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	topPositiveReviews := l.NewTopPositiveReviews(middleware.ReceiveMsg, middleware.SendQueryResults)
	topPositiveReviews.Run(indieReviewJoinersAmount)
}
