package entrypoint

import (
	"net"

	"github.com/op/go-logging"

	cp "distribuidos-tp/internal/client_protocol"
)

var log = logging.MustGetLogger("log")

type SendGamesBatchFunc func(clientID int, data []byte) error
type SendReviewsBatchFunc func(clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, data []byte, currentReviewId int, messagesSent map[int]int) (sentReviewsAmount int, e error)
type SendGamesEndOfFileFunc func(clientID int) error
type SendReviewsEndOfFileFunc func(clientID int, negativeReviewsPreFiltersAmount int, reviewMappersAmount int, sentMessages map[int]int) error
type ReceiveQueryResultFunc func() (rawMessage []byte, e error)

type EntryPoint struct {
	SendGamesBatch       SendGamesBatchFunc
	SendReviewsBatch     SendReviewsBatchFunc
	SendGamesEndOfFile   SendGamesEndOfFileFunc
	SendReviewsEndOfFile SendReviewsEndOfFileFunc
	ReceiveQueryResult   ReceiveQueryResultFunc
}

func NewEntryPoint(
	sendGamesBatch SendGamesBatchFunc,
	sendReviewsBatch SendReviewsBatchFunc,
	sendGamesEndOfFile SendGamesEndOfFileFunc,
	sendReviewsEndOfFile SendReviewsEndOfFileFunc,
	receiveQueryResponse ReceiveQueryResultFunc,
) *EntryPoint {
	return &EntryPoint{
		SendGamesBatch:       sendGamesBatch,
		SendReviewsBatch:     sendReviewsBatch,
		SendGamesEndOfFile:   sendGamesEndOfFile,
		SendReviewsEndOfFile: sendReviewsEndOfFile,
		ReceiveQueryResult:   receiveQueryResponse,
	}
}

func (e *EntryPoint) Run(conn net.Conn, clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int) {
	eofGames := false
	eofReviews := false

	messagesSent := make(map[int]int)
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
			reviewsSent, err := e.SendReviewsBatch(clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount, data, currentReviewId, messagesSent)
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
				err = e.SendReviewsEndOfFile(clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount, messagesSent)
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
