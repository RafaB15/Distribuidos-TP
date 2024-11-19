package entrypoint

import (
	"net"

	"github.com/op/go-logging"

	cp "distribuidos-tp/internal/client_protocol"
)

var log = logging.MustGetLogger("log")

type EntryPoint struct {
	SendGamesBatch       func(int, []byte) error
	SendReviewsBatch     func(int, int, int, []byte, int) (int, error)
	SendGamesEndOfFile   func(int) error
	SendReviewsEndOfFile func(int, int, int) error
	ReceiveQueryResult   func() ([]byte, error)
}

func NewEntryPoint(
	sendGamesBatch func(int, []byte) error,
	sendReviewsBatch func(int, int, int, []byte, int) (int, error),
	sendGamesEndOfFile func(int) error,
	sendReviewsEndOfFile func(int, int, int) error,
	receiveQueryResponse func() ([]byte, error),
) *EntryPoint {
	return &EntryPoint{
		SendGamesBatch:       sendGamesBatch,
		SendReviewsBatch:     sendReviewsBatch,
		SendGamesEndOfFile:   sendGamesEndOfFile,
		SendReviewsEndOfFile: sendReviewsEndOfFile,
		ReceiveQueryResult:   receiveQueryResponse,
	}
}

func (e *EntryPoint) Run(conn net.Conn, clientID int, negativeReviewsPreFiltersAmount int, reviewAccumulatorsAmount int) {
	eofGames := false
	eofReviews := false

	currentReviewId := 0

	for !eofGames || !eofReviews {
		data, fileOrigin, eofFlag, err := cp.ReceiveBatch(conn)
		if err != nil {
			log.Errorf("Error receiving game batch for client %d: %v", clientID, err)
			return
		}

		if fileOrigin == cp.GameFile {
			err = e.SendGamesBatch(clientID, data)
			if err != nil {
				log.Errorf("Error sending batch for client %d: %v", clientID, err)
				return
			}
		} else {
			reviewsSent, err := e.SendReviewsBatch(clientID, negativeReviewsPreFiltersAmount, reviewAccumulatorsAmount, data, currentReviewId)
			if err != nil {
				log.Errorf("Error sending batch for client %d: %v", clientID, err)
				return
			}
			currentReviewId += reviewsSent
		}

		if eofFlag {
			if fileOrigin == cp.GameFile {
				err = e.SendGamesEndOfFile(clientID)
				log.Infof("End of file message published for games with clientID %d", clientID)
				eofGames = true
			} else {
				err = e.SendReviewsEndOfFile(clientID, negativeReviewsPreFiltersAmount, reviewAccumulatorsAmount)
				log.Infof("End of file message published for reviews with clientID %d", clientID)
				eofReviews = true
			}
			if err != nil {
				log.Errorf("Error sending end of file message for client %d: %v", clientID, err)
				return
			}
		}
	}

	remainingQueries := 5

	for remainingQueries > 0 {
		result, err := e.ReceiveQueryResult()
		if err != nil {
			log.Errorf("Error receiving query result for client %d: %v", clientID, err)
			return
		}

		totalWritten := 0
		for totalWritten < len(result) {
			n, err := conn.Write([]byte(result[totalWritten:]))
			if err != nil {
				log.Errorf("Failed to send message to client %d: %v", clientID, err)
				return
			}
			totalWritten += n
		}

		remainingQueries--
	}

	log.Infof("Client %d finished", clientID)
}
