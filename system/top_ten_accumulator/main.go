package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/top_ten_accumulator/logic"
	m "distribuidos-tp/system/top_ten_accumulator/middleware"
	p "distribuidos-tp/system/top_ten_accumulator/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	DecadeFiltersAmountEnvironmentVariableName = "DECADE_FILTERS_AMOUNT"
	FileName                                   = "top_ten_games"
)

var log = logging.MustGetLogger("log")

func main() {
	go u.HandlePing()
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	filtersAmount, err := u.GetEnvInt(DecadeFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	log.Infof("Starting Top Ten Accumulator")
	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	topTenAccumulator := l.NewTopTenAccumulator(middleware.ReceiveMsg, middleware.SendMsg, middleware.AckLastMessage, log)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		topTenAccumulator.Run(filtersAmount, repository)
		doneChannel <- true
	}()

	<-doneChannel
}
