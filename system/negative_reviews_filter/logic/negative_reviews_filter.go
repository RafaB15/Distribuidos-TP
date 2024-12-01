package negative_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/negative_reviews_filter/persistence"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	AckBatchSize = 1
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

func (f *NegativeReviewsFilter) Run(englishReviewAccumulatorsAmount int, minNegativeReviews int, repository *p.Repository) {
	negativeReviewsMap, messageTracker, syncNumber, err := repository.LoadAll(englishReviewAccumulatorsAmount)
	if err != nil {
		f.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {

		clientID, gameReviewsMetrics, eof, newMessage, err := f.ReceiveGameReviewsMetrics(messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientNegativeReviews, exists := negativeReviewsMap.Get(clientID)
		if !exists {
			clientNegativeReviews = make([]*ra.NamedGameReviewsMetrics, 0)
			negativeReviewsMap.Set(clientID, clientNegativeReviews)
		}

		if newMessage && !eof {
			f.logger.Infof("Received game reviews metrics for client %d", clientID)
			for _, currentGameReviewsMetrics := range gameReviewsMetrics {
				f.logger.Infof("Received review with negative reviews: %d", currentGameReviewsMetrics.NegativeReviews)
				if currentGameReviewsMetrics.NegativeReviews >= minNegativeReviews {
					f.logger.Infof("Client %d has a game with negative reviews: %s", clientID, currentGameReviewsMetrics.Name)
					clientNegativeReviews = append(clientNegativeReviews, currentGameReviewsMetrics)
					f.logger.Infof("Neagtive filtered: %d", len(clientNegativeReviews))
				}
			}

			negativeReviewsMap.Set(clientID, clientNegativeReviews)
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending query results")
			err = f.SendQueryResults(clientID, clientNegativeReviews)
			if err != nil {
				f.logger.Errorf("Failed to send game reviews metrics: %v", err)
				return
			}
			f.logger.Infof("Sent final result of client: %d", clientID)

			messageTracker.DeleteClientInfo(clientID)
			negativeReviewsMap.Delete(clientID)

			syncNumber++
			err = repository.SaveAll(negativeReviewsMap, messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			syncNumber++
			err = repository.SaveAll(negativeReviewsMap, messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save data: %v", err)
				return
			}

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
