package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/action_english_review_joiner/logic"
	m "distribuidos-tp/system/action_english_review_joiner/middleware"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName                           = "ID"                              //al middleware
	NegativeReviewsFiltersAmountEnvironmentVariableName = "NEGATIVE_REVIEWS_FILTERS_AMOUNT" //al run

)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	negativeReviewsFiltersAmount, err := u.GetEnvInt(NegativeReviewsFiltersAmountEnvironmentVariableName)
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

	actionEnglishReviewJoiner := l.NewActionEnglishReviewJoiner(middleware.ReceiveMsg, middleware.SendMetrics, middleware.SendEof)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		actionEnglishReviewJoiner.Run(negativeReviewsFiltersAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
