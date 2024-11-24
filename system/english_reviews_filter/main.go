package main

import (
	u "distribuidos-tp/internal/utils"
	"os"
	"os/signal"
	"syscall"

	l "distribuidos-tp/system/english_reviews_filter/logic"
	m "distribuidos-tp/system/english_reviews_filter/middleware"

	"github.com/op/go-logging"
)

const (
	AccumulatorsAmountEnvironmentVariableName        = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                        = "ID"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

func main() {
	var log = logging.MustGetLogger("log")

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	actionReviewJoinersAmount, err := u.GetEnvInt(ActionReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	englishReviewsFilter := l.NewEnglishReviewsFilter(
		middleware.ReceiveGameReviews,
		middleware.SendEnglishReview,
		middleware.SendEndOfFiles,
		middleware.AckLastMessage,
		log,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		englishReviewsFilter.Run(accumulatorsAmount, actionReviewJoinersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
