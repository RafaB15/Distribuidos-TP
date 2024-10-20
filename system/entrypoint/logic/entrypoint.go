package entrypoint

import (
	"net"

	"github.com/op/go-logging"

	cp "distribuidos-tp/internal/client_protocol"
)

var log = logging.MustGetLogger("log")

type EntryPoint struct {
	SendGamesBatch       func(int, []byte) error
	SendReviewsBatch     func(int, []byte) error
	SendGamesEndOfFile   func(int) error
	SendReviewsEndOfFile func(int, int, int) error
	ReceiveQueryResult   func() ([]byte, error)
}

func NewEntryPoint(
	sendGamesBatch func(int, []byte) error,
	sendReviewsBatch func(int, []byte) error,
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

func (e *EntryPoint) Run(conn net.Conn, clientID int, englishFiltersAmount int, reviewMappersAmount int) {
	eofGames := false
	eofReviews := false

	for !eofGames || !eofReviews {
		data, fileOrigin, eofFlag, err := cp.ReceiveBatch(conn)
		if err != nil {
			log.Errorf("Error receiving game batch for client %d: %v", clientID, err)
			return
		}

		if fileOrigin == cp.GameFile {
			log.Infof("Received game batch for client %d", clientID)
			err = e.SendGamesBatch(clientID, data)
		} else {
			log.Infof("Received review batch for client %d", clientID)
			err = e.SendReviewsBatch(clientID, data)
		}

		if err != nil {
			log.Errorf("Error sending batch for client %d: %v", clientID, err)
			return
		}

		if eofFlag {
			if fileOrigin == cp.GameFile {
				err = e.SendGamesEndOfFile(clientID)
				log.Infof("End of file message published for games with clientID %d", clientID)
				eofGames = true
			} else {
				err = e.SendReviewsEndOfFile(clientID, englishFiltersAmount, reviewMappersAmount)
				log.Infof("End of file message published for reviews with clientID %d", clientID)
				eofReviews = true
			}
			if err != nil {
				log.Errorf("Error sending end of file message for client %d: %v", clientID, err)
				return
			}
		}
	}

	remainingQueires := 5

	for remainingQueires > 0 {
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

		remainingQueires--
	}

	log.Infof("Client %d finished", clientID)
}
