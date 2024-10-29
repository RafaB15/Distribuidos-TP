package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/positive_reviews_filter/logic"
	m "distribuidos-tp/system/positive_reviews_filter/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	ActionPositiveReviewsJoinersAmountEnvironmentVariableName = "ACTION_POSITIVE_REVIEWS_JOINERS_AMOUNT"
	EnglishReviewAccumulatorsAmountEnvironmentVariableName    = "ENGLISH_REVIEW_ACCUMULATORS_AMOUNT"
	MinPositiveReviews                                        = 5000
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	actionReviewsJoinersAmount, err := u.GetEnvInt(ActionPositiveReviewsJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	englishReviewAccumulatorsAmount, err := u.GetEnvInt(EnglishReviewAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	positiveReviewsFilter := l.NewPositiveReviewsFilter(
		middleware.ReceiveGameReviewsMetrics,
		middleware.SendGameReviewsMetrics,
		middleware.SendEndOfFiles,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		positiveReviewsFilter.Run(actionReviewsJoinersAmount, englishReviewAccumulatorsAmount, MinPositiveReviews)
		doneChannel <- true
	}()

	<-doneChannel
}
