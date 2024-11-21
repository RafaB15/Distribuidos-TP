package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/os_accumulator/logic"
	m "distribuidos-tp/system/os_accumulator/middleware"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	IdEnvironmentVariableName = "ID"
)

var log = logging.MustGetLogger("log")

func main() {

	go handlePing()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	id, err := u.GetEnvInt(IdEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	middleware, err := m.NewMiddleware(id)
	if err != nil {
		log.Errorf("failed to create middleware: %v", err)
		return
	}

	// Ejecutar el acumulador con el contexto
	osAccumulator := l.NewOSAccumulator(middleware.ReceiveGameOS, middleware.SendMetrics, middleware.SendEof)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		osAccumulator.Run()
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
