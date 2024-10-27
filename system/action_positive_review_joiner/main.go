package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/action_positive_review_joiner/logic"
	m "distribuidos-tp/system/action_positive_review_joiner/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                           = "ID"                              //al middleware
	PositiveReviewsFiltersAmountEnvironmentVariableName = "POSITIVE_REVIEWS_FILTERS_AMOUNT" //al run

)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	positiveReviewsFiltersAmount, err := u.GetEnvInt(PositiveReviewsFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

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

	positiveActionReviewJoiner := l.NewActionPositiveReviewJoiner(middleware.ReceiveMsg, middleware.SendMetrics, middleware.SendEof)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		positiveActionReviewJoiner.Run(positiveReviewsFiltersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
