package main

import (
	u "distribuidos-tp/internal/utils"

	l "distribuidos-tp/system/review_mapper/logic"
	m "distribuidos-tp/system/review_mapper/middleware"

	"github.com/op/go-logging"
)

const (
	AccumulatorsAmountEnvironmentVariableName = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                 = "ID"
)

var log = logging.MustGetLogger("log")

func main() {
	_, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	gameMapper := l.NewReviewMapper(middleware.ReceiveGameReviews, middleware.SendReviews, middleware.SendEndOfFiles)
	gameMapper.Run(accumulatorsAmount)
}
