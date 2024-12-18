package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/english_reviews_accumulator/logic"
	m "distribuidos-tp/system/english_reviews_accumulator/middleware"
	p "distribuidos-tp/system/english_reviews_accumulator/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName            = "ID"
	FiltersAmountEnvironmentVariableName = "FILTERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	go u.HandlePing()

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

	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	englishReviewsAccumulator := l.NewEnglishReviewsAccumulator(
		middleware.ReceiveReview,
		middleware.SendAccumulatedReviews,
		middleware.SendEndOfFiles,
		middleware.SendDeleteClient,
		middleware.AckLastMessages,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		englishReviewsAccumulator.Run(id, filtersAmount, repository)
		doneChannel <- true
	}()

	<-doneChannel
}
