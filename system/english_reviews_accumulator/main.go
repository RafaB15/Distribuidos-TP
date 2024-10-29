package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/english_reviews_accumulator/logic"
	m "distribuidos-tp/system/english_reviews_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                          = "ID"
	FiltersAmountEnvironmentVariableName               = "FILTERS_AMOUNT"
	PositiveReviewsFilterAmountEnvironmentVariableName = "POSITIVE_REVIEWS_FILTER_AMOUNT"
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

	filtersAmount, err := u.GetEnvInt(FiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	positiveReviewsFilterAmount, err := u.GetEnvInt(PositiveReviewsFilterAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	englishReviewsAccumulator := l.NewEnglishReviewsAccumulator(
		middleware.ReceiveReviews,
		middleware.SendAccumulatedReviews,
		middleware.SendEndOfFiles,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		englishReviewsAccumulator.Run(filtersAmount, positiveReviewsFilterAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
