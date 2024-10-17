package main

import (
	l "distribuidos-tp/system/indie_review_joiner/logic"
	m "distribuidos-tp/system/indie_review_joiner/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	reviewJoiner := l.NewIndieReviewJoiner(middleware.ReceiveMsg, m.HandleGameReviewMetrics, m.HandleGameNames, m.GetAccumulatorsAmount, middleware.SendMetrics, middleware.SendEof)
	reviewJoiner.Run()
}
