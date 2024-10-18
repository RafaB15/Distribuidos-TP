package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/action_negative_review_joiner/logic"
	m "distribuidos-tp/system/action_negative_review_joiner/middleware"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName = "ID"
)

var log = logging.MustGetLogger("log")

func main() {

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

	negativeActionReviewJoiner := l.NewActionNegativeReviewJoiner(middleware.ReceiveMsg, m.HandleGameReviewMetrics, m.HandleGameNames, middleware.SendMetrics, middleware.SendEof)
	negativeActionReviewJoiner.Run()
}
