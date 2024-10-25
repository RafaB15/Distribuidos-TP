package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/final_positive_joiner/logic"
	m "distribuidos-tp/system/final_positive_joiner/middleware"

	"github.com/op/go-logging"
)

const (
	ActionPositiveJoinersAmountEnvironmentVariableName = "ACTION_POSITIVE_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	actionPositiveJoinersAmount, err := u.GetEnvInt(ActionPositiveJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	finalPositiveJoiner := l.NewFinalPositiveJoiner(middleware.ReceiveJoinedGameReviews, middleware.SendQueryResults)
	finalPositiveJoiner.Run(actionPositiveJoinersAmount)
}
