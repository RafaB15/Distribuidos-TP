package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/final_negative_joiner/logic"
	m "distribuidos-tp/system/final_negative_joiner/middleware"

	"github.com/op/go-logging"
)

const (
	ActionNegativeJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	actionNegativeJoinersAmount, err := u.GetEnvInt(ActionNegativeJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	finalNegativeJoiner := l.NewFinalNegativeJoiner(middleware.ReceiveJoinedGameReviews, middleware.SendQueryResults, middleware.SendEof)
	finalNegativeJoiner.Run(actionNegativeJoinersAmount)
}
