package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_final_accumulator/logic"
	m "distribuidos-tp/system/os_final_accumulator/middleware"
	p "distribuidos-tp/system/os_final_accumulator/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const OSAccumulatorsAmountEnvironmentVariableName = "NUM_PREVIOUS_OS_ACCUMULATORS"

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	osAccumulatorsAmount, err := u.GetEnvInt(OSAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	osAccumulator := l.NewOSFinalAccumulator(
		middleware.ReceiveGamesOSMetrics,
		middleware.SendFinalMetrics,
		middleware.AckLastMessage,
		log,
	)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		osAccumulator.Run(osAccumulatorsAmount, repository)
		doneChannel <- true
	}()

	<-doneChannel

}
