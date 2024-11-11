package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/reviews_accumulator/logic"
	m "distribuidos-tp/system/reviews_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName      = "ID"
	IndieReviewJoinersAmountName   = "INDIE_REVIEW_JOINERS_AMOUNT"
	NegativeReviewPreFiltersAmount = "NEGATIVE_REVIEWS_PRE_FILTERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	negativeReviewPreFiltersAmount, err := u.GetEnvInt(NegativeReviewPreFiltersAmount)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	reviewsAccumulator := l.NewReviewsAccumulator(middleware.ReceiveReviews, middleware.SendAccumulatedReviews, middleware.SendEof)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		reviewsAccumulator.Run(indieReviewJoinersAmount, negativeReviewPreFiltersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
