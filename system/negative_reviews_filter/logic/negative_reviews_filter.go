package negative_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReceiveGameReviewsMetricsFunc func() (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, err error)
type SendQueryResultsFunc func(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error

type NegativeReviewsFilter struct {
	ReceiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc
	SendQueryResults          SendQueryResultsFunc
}

func NewNegativeReviewsFilter(
	receiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc,
	sendQueryResults SendQueryResultsFunc,
) *NegativeReviewsFilter {
	return &NegativeReviewsFilter{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendQueryResults:          sendQueryResults,
	}
}

func (f *NegativeReviewsFilter) Run(englishReviewAccumulatorsAmount int, minNegativeReviews int) {
	remainingEOFsMap := make(map[int]int)
	negativeReviewsMap := make(map[int][]*ra.NamedGameReviewsMetrics)

	for {

		clientID, gameReviewsMetrics, eof, err := f.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientNegativeReviews, exists := negativeReviewsMap[clientID]
		if !exists {
			clientNegativeReviews = make([]*ra.NamedGameReviewsMetrics, 0)
			negativeReviewsMap[clientID] = clientNegativeReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)
			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = englishReviewAccumulatorsAmount
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			log.Infof("Received all EOFs for client %d", clientID)

			err = f.SendQueryResults(clientID, clientNegativeReviews)
			if err != nil {
				log.Errorf("Failed to send game reviews metrics: %v", err)
				return
			}

			log.Infof("Sent final result of client: %d", clientID)

			delete(negativeReviewsMap, clientID)
			delete(remainingEOFsMap, clientID)

			continue
		}

		for _, currentGameReviewsMetrics := range gameReviewsMetrics {
			if currentGameReviewsMetrics.NegativeReviews >= minNegativeReviews {
				clientNegativeReviews = append(clientNegativeReviews, currentGameReviewsMetrics)
			}
		}

		negativeReviewsMap[clientID] = clientNegativeReviews
	}
}
