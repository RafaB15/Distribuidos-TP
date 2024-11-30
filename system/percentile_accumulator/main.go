package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/percentile_accumulator/logic"
	m "distribuidos-tp/system/percentile_accumulator/middleware"
	p "distribuidos-tp/system/percentile_accumulator/persistence"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	NumPreviousAccumulators = "NUM_PREVIOUS_ACCUMULATORS"
)

var log = logging.MustGetLogger("log")

func main() {
	go handlePing()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	previousAccumulators, err := u.GetEnvInt(NumPreviousAccumulators)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	log.Info("Starting Percentile Accumulator")
	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	positiveReviewsFilter := l.NewPercentileAccumulator(
		middleware.ReceiveGameReviewsMetrics,
		middleware.SendQueryResults,
		middleware.AckLastMessage,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		positiveReviewsFilter.Run(previousAccumulators, repository)
		doneChannel <- true
	}()

	<-doneChannel
}

func handlePing() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// fmt.Fprintln(w, "Pong")
	})

	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
