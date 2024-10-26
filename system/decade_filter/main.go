package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/decade_filter/logic"
	m "distribuidos-tp/system/decade_filter/middleware"
	"github.com/op/go-logging"
	"os"
	"os/signal"
	"syscall"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	log.Infof("Starting Decade Filter")
	middleware, err := m.NewMiddleware()
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	decadeFilter := l.NewDecadeFilter(middleware.ReceiveYearAvgPtf, middleware.SendFilteredYearAvgPtf, middleware.SendEof)

	go func() {
		decadeFilter.Run()
		doneChannel <- true
	}()

	<-doneChannel
}
