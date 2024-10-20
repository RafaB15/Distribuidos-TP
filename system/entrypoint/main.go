package main

import (
	u "distribuidos-tp/internal/utils"

	l "distribuidos-tp/system/entrypoint/logic"
	m "distribuidos-tp/system/entrypoint/middleware"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const (
	port = ":3000"

	GameFile   = 1
	ReviewFile = 0

	EnglishFiltersAmountEnvironmentVariableName = "ENGLISH_FILTERS_AMOUNT"
	ReviewMappersAmountEnvironmentVariableName  = "REVIEW_MAPPERS_AMOUNT"
)

var log = logging.MustGetLogger("log")

func main() {
	englishFiltersAmount, err := u.GetEnvInt(EnglishFiltersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	reviewMappersAmount, err := u.GetEnvInt(ReviewMappersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	listener, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	clientID := 1
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			return
		}

		go handleConnection(conn, englishFiltersAmount, reviewMappersAmount, clientID)
	}

}

func handleConnection(conn net.Conn, englishFiltersAmount int, reviewMappersAmount int, clientID int) {
	defer conn.Close()

	middleware, err := m.NewMiddleware()
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

	entryPoint.Run(conn, clientID, englishFiltersAmount, reviewMappersAmount)
}
