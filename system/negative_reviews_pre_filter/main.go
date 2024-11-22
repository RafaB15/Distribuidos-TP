package main

import (
	u "distribuidos-tp/internal/utils"
	"os"
	"os/signal"
	"sync"
	"syscall"

	l "distribuidos-tp/system/negative_reviews_pre_filter/logic"
	m "distribuidos-tp/system/negative_reviews_pre_filter/middleware"
	p "distribuidos-tp/system/negative_reviews_pre_filter/persistence"

	"github.com/op/go-logging"
)

const (
	EnglishFiltersAmountEnvironmentVariableName = "ENGLISH_FILTERS_AMOUNT"
	AccumulatorsAmountEnvironmentVariableName   = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                   = "ID"
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

	englishFiltersAmount, err := u.GetEnvInt(EnglishFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	accumulatorsAmount, err := u.GetEnvInt(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	negativeReviewsPreFilter := l.NewNegativeReviewsPreFilter(
		middleware.ReceiveMessage,
		middleware.SendRawReview,
		middleware.AckLastMessage,
		middleware.SendEndOfFile,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		negativeReviewsPreFilter.Run(id, repository, englishFiltersAmount, accumulatorsAmount)
		doneChannel <- true
	}()

	<-doneChannel
}
