package main

import (
	u "distribuidos-tp/internal/utils"
	"os"
	"os/signal"
	"sync"
	"syscall"

	l "distribuidos-tp/system/entrypoint/logic"
	m "distribuidos-tp/system/entrypoint/middleware"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const (
	port                                                   = ":3000"
	NegativeReviewsPreFiltersAmountEnvironmentVariableName = "NEGATIVE_REVIEWS_PRE_FILTERS_AMOUNT"
	ReviewAccumulatorsAmountEnvironmentVariableName        = "REVIEW_ACCUMULATORS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	var mainWG sync.WaitGroup

	negativeReviewsPreFiltersAmount, err := u.GetEnvInt(NegativeReviewsPreFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	reviewAccumulatorsAmount, err := u.GetEnvInt(ReviewAccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	go handleMainGracefulShutdown(listener, signalChannel, &mainWG)

	clientID := 1
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			break
		}

		mainWG.Add(1)
		go handleConnection(conn, negativeReviewsPreFiltersAmount, reviewAccumulatorsAmount, clientID, &mainWG)
		clientID++
	}

	mainWG.Wait()
}

func handleConnection(conn net.Conn, negativeReviewsPreFiltersAmount int, reviewAccumulatorsAmount int, clientID int, mainWG *sync.WaitGroup) {
	defer mainWG.Done()
	defer conn.Close()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	middleware, err := m.NewMiddleware(clientID)
	if err != nil {
		fmt.Printf("Error creating middleware: %v for client %d\n", err, clientID)
		return
	}

	entryPoint := l.NewEntryPoint(
		middleware.SendGamesBatch,
		middleware.SendReviewsBatch,
		middleware.SendGamesEndOfFile,
		middleware.SendReviewsEndOfFile,
		middleware.ReceiveQueryResponse,
	)

	var clientWG sync.WaitGroup
	go handleClientGracefulShutdown(clientID, conn, &clientWG, middleware, signalChannel, doneChannel)

	go func() {
		entryPoint.Run(conn, clientID, negativeReviewsPreFiltersAmount, reviewAccumulatorsAmount)
		doneChannel <- true
	}()

	clientWG.Wait()
	<-doneChannel
}

func handleMainGracefulShutdown(listener net.Listener, signalChannel chan os.Signal, wg *sync.WaitGroup) {
	<-signalChannel
	log.Info("Received termination signal. Starting graceful shutdown...")
	wg.Wait()
	if err := listener.Close(); err != nil {
		log.Errorf("Error closing listener: %v", err)
	}
	log.Info("Graceful shutdown completed.")
}

func handleClientGracefulShutdown(clientID int, conn net.Conn, clientWG *sync.WaitGroup, middleware *m.Middleware, signalChannel chan os.Signal, doneChannel chan bool) {
	<-signalChannel
	clientWG.Add(1)
	log.Infof("Received termination signal for client %d. Starting graceful shutdown...", clientID)

	if err := middleware.Close(); err != nil {
		log.Errorf("Error closing middleware for client %d: %v", clientID, err)
	}

	err := conn.Close()
	if err != nil {
		log.Errorf("Error closing connection for client %d: %v", clientID, err)
	}

	log.Infof("Graceful shutdown completed for client %d.", clientID)
	clientWG.Done()
	doneChannel <- true
}
