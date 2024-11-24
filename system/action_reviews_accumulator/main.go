package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/action_reviews_accumulator/logic"
	m "distribuidos-tp/system/action_reviews_accumulator/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                        = "ID"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
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

	actionReviewsAccumulator := l.NewActionReviewsAccumulator(
		middleware.ReceiveReview,
		middleware.SendAccumulatedReviews,
		middleware.SendEndOfFiles,
		middleware.AckLastMessages,
		log,
	)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		actionReviewsAccumulator.Run(actionReviewJoinersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}