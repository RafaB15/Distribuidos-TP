package main

import (
	l "distribuidos-tp/system/reviews_accumulator/logic"
	m "distribuidos-tp/system/reviews_accumulator/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	reviewsAccumulator := l.NewReviewsAccumulator(middleware.ReceiveReviews, middleware.SendAccumulatedReviews, m.GetAccumulatorsAmount, middleware.SendEof)
	reviewsAccumulator.Run()
}
