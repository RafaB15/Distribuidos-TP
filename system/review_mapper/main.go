package main

import (
	l "distribuidos-tp/system/review_mapper/logic"
	m "distribuidos-tp/system/review_mapper/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	gameMapper := l.NewReviewMapper(middleware.ReceiveReviewBatch, m.GetAccumulatorsAmount, middleware.SendMetrics, middleware.SendEof)
	gameMapper.Run()
}
