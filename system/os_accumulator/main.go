package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_accumulator/logic"
	m "distribuidos-tp/system/os_accumulator/middleware"
	p "distribuidos-tp/system/os_accumulator/persistence"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName = "ID"
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

	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("failed to create middleware: %v", err)
		return
	}

	osAccumulator := l.NewOSAccumulator(middleware.ReceiveGameOS, middleware.SendMetrics, middleware.SendEof, middleware.SendDeleteClient, middleware.AckLastMessage, log)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdownWithWaitGroup(&wg, middleware, signalChannel, doneChannel, log)

	go func() {
		osAccumulator.Run(id, repository)
		doneChannel <- true
	}()

	<-doneChannel

}
