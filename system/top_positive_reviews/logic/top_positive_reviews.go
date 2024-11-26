package top_positive_reviews

import (
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"

	"sort"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 50
)

type ReceiveMsgFunc func(messageTracker *n.MessageTracker) (clientID int, reviews *j.JoinedPositiveGameReview, eof bool, newMessage bool, e error)
type SendQueryResultsFunc func(clientID int, reviews []*j.JoinedPositiveGameReview) error
type AckLastMessageFunc func() error
type TopPositiveReviews struct {
	ReceiveMsg       ReceiveMsgFunc
	SendQueryResults SendQueryResultsFunc
	AckLastMessage   AckLastMessageFunc
	logger           *logging.Logger
}

func NewTopPositiveReviews(receiveMsg ReceiveMsgFunc, sendMetrics SendQueryResultsFunc, ackLastMessage AckLastMessageFunc, logger *logging.Logger) *TopPositiveReviews {
	return &TopPositiveReviews{
		ReceiveMsg:       receiveMsg,
		SendQueryResults: sendMetrics,
		AckLastMessage:   ackLastMessage,
		logger:           logger,
	}
}

func (t *TopPositiveReviews) Run(indieReviewJoinerAmount int) {
	messageTracker := n.NewMessageTracker(indieReviewJoinerAmount)
	messagesUntilAck := AckBatchSize

	remainingEOFsMap := make(map[int]int)
	topPositiveIndieGames := make(map[int][]*j.JoinedPositiveGameReview)

	for {
		clientID, msg, eof, newMessage, err := t.ReceiveMsg(messageTracker)
		if err != nil {
			t.logger.Errorf("Failed to receive message: %v", err)
			return
		}

		clientTopPositiveIndieGames, exists := topPositiveIndieGames[clientID]
		if !exists {
			clientTopPositiveIndieGames = []*j.JoinedPositiveGameReview{}
			topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
		}

		if newMessage && !eof {
			t.logger.Infof("Received indie game with ID: %v", msg.AppId)
			t.logger.Infof("Evaluating number of positive reviews and saving game")
			clientTopPositiveIndieGames = append(clientTopPositiveIndieGames, msg)
			topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
			if len(clientTopPositiveIndieGames) > 5 {
				// Sort the slice by positive reviews in descending order
				sort.Slice(clientTopPositiveIndieGames, func(i, j int) bool {
					return clientTopPositiveIndieGames[i].PositiveReviews > clientTopPositiveIndieGames[j].PositiveReviews
				})
				// Keep only the top 5
				clientTopPositiveIndieGames = clientTopPositiveIndieGames[:5]
				topPositiveIndieGames[clientID] = clientTopPositiveIndieGames
			}

		}

		if messageTracker.ClientFinished(clientID, t.logger) {
			t.logger.Infof("Client %d finished", clientID)
			t.logger.Infof("Sending Query Results to client %d", clientID)

			err = t.SendQueryResults(clientID, clientTopPositiveIndieGames)
			if err != nil {
				t.logger.Errorf("Failed to send metrics: %v", err)
				return
			}
			t.logger.Infof("Sent Top 5 positive reviews to writer")
			delete(topPositiveIndieGames, clientID)
			delete(remainingEOFsMap, clientID)
			messageTracker.DeleteClientInfo(clientID)
			messagesUntilAck = AckBatchSize
			err = t.AckLastMessage()
			if err != nil {
				t.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			err = t.AckLastMessage()
			if err != nil {
				t.logger.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}

	}

}
