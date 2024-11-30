package negative_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	AckBatchSize = 10
)

type ReceiveGameReviewsMetricsFunc func(messageTracker *n.MessageTracker) (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, newMessage bool, err error)
type SendQueryResultsFunc func(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error
type AckLastMessageFunc func() error

type NegativeReviewsFilter struct {
	ReceiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc
	SendQueryResults          SendQueryResultsFunc
	AckLastMessage            AckLastMessageFunc
	logger                    *logging.Logger
}

func NewNegativeReviewsFilter(
	receiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc,
	sendQueryResults SendQueryResultsFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *NegativeReviewsFilter {
	return &NegativeReviewsFilter{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendQueryResults:          sendQueryResults,
		AckLastMessage:            ackLastMessage,
		logger:                    logger,
	}
}

func (f *NegativeReviewsFilter) Run(englishReviewAccumulatorsAmount int, minNegativeReviews int) {
	messageTracker := n.NewMessageTracker(englishReviewAccumulatorsAmount)
	negativeReviewsMap := make(map[int][]*ra.NamedGameReviewsMetrics)

	messagesUntilAck := AckBatchSize

	for {

		clientID, gameReviewsMetrics, eof, newMessage, err := f.ReceiveGameReviewsMetrics(messageTracker)
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientNegativeReviews, exists := negativeReviewsMap[clientID]
		if !exists {
			clientNegativeReviews = make([]*ra.NamedGameReviewsMetrics, 0)
			negativeReviewsMap[clientID] = clientNegativeReviews
		}

		if newMessage && !eof {
			log.Infof("Received game reviews metrics for client %d", clientID)
			for _, currentGameReviewsMetrics := range gameReviewsMetrics {
				log.Infof("Received review with negative reviews: %d", currentGameReviewsMetrics.NegativeReviews)
				if currentGameReviewsMetrics.NegativeReviews >= minNegativeReviews {
					log.Infof("Client %d has a game with negative reviews: %s", clientID, currentGameReviewsMetrics.Name)
					clientNegativeReviews = append(clientNegativeReviews, currentGameReviewsMetrics)
					log.Infof("Neagtive filtered: %d", len(clientNegativeReviews))
				}
			}

			negativeReviewsMap[clientID] = clientNegativeReviews
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			log.Infof("Client %d finished", clientID)

			log.Info("Sending query results")
			err = f.SendQueryResults(clientID, clientNegativeReviews)
			if err != nil {
				log.Errorf("Failed to send game reviews metrics: %v", err)
				return
			}
			log.Infof("Sent final result of client: %d", clientID)

			messageTracker.DeleteClientInfo(clientID)
			delete(negativeReviewsMap, clientID)

			messagesUntilAck = AckBatchSize
			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			messagesUntilAck = AckBatchSize
			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		} else {
			messagesUntilAck--
		}

	}
}
