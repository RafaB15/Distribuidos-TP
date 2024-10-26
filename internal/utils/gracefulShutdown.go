package utils

import (
	"github.com/op/go-logging"
	"os"
)

var log = logging.MustGetLogger("log")

// Closable defines an interface with a Close method
type Closable interface {
	Close() error
}

func HandleGracefulShutdown(middleware Closable, signalChannel chan os.Signal, doneChannel chan bool) {
	<-signalChannel
	log.Info("Received termination signal. Starting graceful shutdown...")
	if err := middleware.Close(); err != nil {
		log.Errorf("Error closing middleware: %v", err)
	}
	log.Info("Graceful shutdown completed.")
	doneChannel <- true
}
