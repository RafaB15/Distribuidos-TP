package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	e "distribuidos-tp/internal/system_protocol/entrypoint"
	u "distribuidos-tp/internal/utils"
	l "distribuidos-tp/system/entrypoint/logic"
	m "distribuidos-tp/system/entrypoint/middleware"
	p "distribuidos-tp/system/entrypoint/persistence"

	"github.com/op/go-logging"
)

const (
	port                                             = ":3000"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
	ReviewAccumulatorsAmountEnvironmentVariableName  = "REVIEW_ACCUMULATORS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	var mainWG sync.WaitGroup

	actionReviewJoinerAmount, err := u.GetEnvInt(ActionReviewJoinersAmountEnvironmentVariableName)
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

	repository := p.NewRepository(&mainWG, log)
	clientTracker := repository.LoadClientTracker()
	deleteUnfinishedClients(clientTracker, actionReviewJoinerAmount, reviewAccumulatorsAmount)

	var mu sync.Mutex

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			break
		}

		mainWG.Add(1)
		mu.Lock()
		currentClientId := clientTracker.AddClient()
		mu.Unlock()
		go handleConnection(conn, actionReviewJoinerAmount, reviewAccumulatorsAmount, currentClientId, &mainWG, clientTracker, repository, &mu)
	}

	mainWG.Wait()
}

func handleConnection(conn net.Conn, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, clientID int, mainWG *sync.WaitGroup, clientTracker *e.ClientTracker, repository *p.Repository, mu *sync.Mutex) {
	defer mainWG.Done()
	defer conn.Close()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM, syscall.SIGINT)

	doneChannel := make(chan bool)

	middleware, err := m.NewMiddleware(clientID, log)
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
		entryPoint.Run(conn, clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount, clientTracker, repository, mu)
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

func deleteUnfinishedClients(clientTracker *e.ClientTracker, actionReviewJoinersAmount int, reviewAccumulatorsAmount int) {
	for clientID := range clientTracker.CurrentClients {
		middleware, err := m.NewMiddleware(clientID, log)
		if err != nil {
			fmt.Printf("Error creating middleware: %v\n", err)
			return
		}
		clientTracker.FinishClient(clientID)
		err = middleware.SendDeleteClient(clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount)
		if err != nil {
			log.Errorf("Error sending delete client message for client %d: %v", clientID, err)
		}
		middleware.EmptyQueryQueue()
	}
}
