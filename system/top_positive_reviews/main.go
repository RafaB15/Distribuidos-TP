package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/top_positive_reviews/logic"
	m "distribuidos-tp/system/top_positive_reviews/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IndieReviewJoinersAmountEnvironmentVariableName = "INDIE_REVIEW_JOINERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	topPositiveReviews := l.NewTopPositiveReviews(middleware.ReceiveMsg, middleware.SendQueryResults, middleware.AckLastMessage, log)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		topPositiveReviews.Run(indieReviewJoinersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
