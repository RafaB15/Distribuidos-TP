package main

import (
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/decade_filter/logic"
	m "distribuidos-tp/system/decade_filter/middleware"
	p "distribuidos-tp/system/decade_filter/persistence"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const IdEnvironmentVariableName = "ID"

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

	log.Infof("Starting Decade Filter")
	middleware, err := m.NewMiddleware(id, log)
	if err != nil {
		log.Errorf("Failed to create middleware: %v", err)
		return
	}

	decadeFilter := l.NewDecadeFilter(middleware.ReceiveYearAvgPtf, middleware.SendFilteredYearAvgPtf, middleware.SendEof, middleware.AckLastMessage, log)

	var wg sync.WaitGroup

	repository := p.NewRepository(&wg, log)

	go u.HandleGracefulShutdown(middleware, signalChannel, doneChannel)

	go func() {
		decadeFilter.Run(id, repository)
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
