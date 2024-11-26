package entrypoint

import (
	n "distribuidos-tp/internal/system_protocol/node"
	"net"

	"github.com/op/go-logging"

	cp "distribuidos-tp/internal/client_protocol"
)

var log = logging.MustGetLogger("log")

type SendGamesBatchFunc func(clientID int, data []byte, messageTracker *n.MessageTracker) error
type SendReviewsBatchFunc func(clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, data []byte, currentReviewId int, messageTracker *n.MessageTracker) (sentReviewsAmount int, e error)
type SendGamesEndOfFileFunc func(clientID int, messageTracker *n.MessageTracker) error
type SendReviewsEndOfFileFunc func(clientID int, actionReviewJoinersAmount int, reviewAccumulatorsAmount int, messageTracker *n.MessageTracker) error
type ReceiveQueryResultFunc func(querysArrived map[int]bool) (rawMessage []byte, repeated bool, e error)

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

	messageTracker := n.NewMessageTracker(0)
	currentReviewId := 0

	for !eofGames || !eofReviews {
		data, fileOrigin, eofFlag, err := cp.ReceiveBatch(conn)
		if err != nil {
			log.Errorf("Error receiving game batch for client %d: %v", clientID, err)
			return
		}

		if fileOrigin == cp.GameFile {
			err = e.SendGamesBatch(clientID, data, messageTracker)
			if err != nil {
				log.Errorf("Error sending batch for client %d: %v", clientID, err)
				return
			}
		} else {
			reviewsSent, err := e.SendReviewsBatch(clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount, data, currentReviewId, messageTracker)
			if err != nil {
				log.Errorf("Error sending batch for client %d: %v", clientID, err)
				return
			}
			currentReviewId += reviewsSent
		}

		if eofFlag {
			if fileOrigin == cp.GameFile {
				err = e.SendGamesEndOfFile(clientID, messageTracker)
				log.Infof("End of file message published for games with clientID %d", clientID)
				eofGames = true
			} else {
				err = e.SendReviewsEndOfFile(clientID, actionReviewJoinersAmount, reviewAccumulatorsAmount, messageTracker)
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

	querysArrived := make(map[int]bool)

	for remainingQueries > 0 {
		result, repeated, err := e.ReceiveQueryResult(querysArrived)
		if err != nil {
			log.Errorf("Error receiving query result for client %d: %v", clientID, err)
			return
		}

		if repeated {
			log.Infof("Client %d received repeated query", clientID)
			continue
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
