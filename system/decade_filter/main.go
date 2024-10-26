package main

import (
	l "distribuidos-tp/system/decade_filter/logic"
	m "distribuidos-tp/system/decade_filter/middleware"
	"github.com/op/go-logging"
	"os"
	"os/signal"
	"syscall"
)

var log = logging.MustGetLogger("log")

func main() {
	doneChannel := make(chan bool)

	log.Infof("Starting Decade Filter")
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	setUpGracefulShutdown(middleware, doneChannel)

	decadeFilter := l.NewDecadeFilter(middleware.ReceiveYearAvgPtf, middleware.SendFilteredYearAvgPtf, middleware.SendEof)

	go func() {
		decadeFilter.Run()
		doneChannel <- true
	}()

	<-doneChannel
}

func setUpGracefulShutdown(middleware *m.Middleware, doneChannel chan bool) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-signalChannel
		log.Info("Received termination signal. Starting graceful shutdown...")
		if err := middleware.Close(); err != nil {
			log.Errorf("Error closing middleware: %v", err)
		}
		log.Info("Graceful shutdown completed.")
		doneChannel <- true
	}()
}
