package negative_reviews_pre_filter

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"github.com/op/go-logging"
)

const (
	MinNegativeReviews = 5000
	AckBatchSize       = 50
)

var log = logging.MustGetLogger("log")

type NegativeReviewsPreFilter struct {
	ReceiveMessage func() (int, []*r.RawReview, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendReview     func(int, int, *r.RawReview) error
	AckLastMessage func() error
	SendEndOfFile  func(int, int) error
}

func NewNegativeReviewsPreFilter(
	receiveMessage func() (int, []*r.RawReview, []*reviews_accumulator.GameReviewsMetrics, bool, error),
	sendReview func(int, int, *r.RawReview) error,
	ackLastMessage func() error,
	sendEndOfFile func(int, int) error,
) *NegativeReviewsPreFilter {
	return &NegativeReviewsPreFilter{
		ReceiveMessage: receiveMessage,
		SendReview:     sendReview,
		AckLastMessage: ackLastMessage,
		SendEndOfFile:  sendEndOfFile,
	}
}

func (f *NegativeReviewsPreFilter) Run(englishFiltersAmount int, accumulatorsAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedRawReviewsMap := make(map[int]map[int][]*r.RawReview)
	gamesToSendMap := make(map[int]map[int]bool)

	messagesUntilAck := AckBatchSize

	for {
		clientID, reviews, gameReviewsMetrics, eof, err := f.ReceiveMessage()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedRawReviews, exists := accumulatedRawReviewsMap[clientID]
		if !exists {
			clientAccumulatedRawReviews = make(map[int][]*r.RawReview)
			accumulatedRawReviewsMap[clientID] = clientAccumulatedRawReviews
		}

		clientGamesToSend, exists := gamesToSendMap[clientID]
		if !exists {
			clientGamesToSend = make(map[int]bool)
			gamesToSendMap[clientID] = clientGamesToSend
		}

		if eof {
			log.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = accumulatorsAmount + 1
			}
			log.Infof("Remaining EOFs: %d", remainingEOFs)
			remainingEOFs--
			log.Infof("Remaining EOFs AFTER: %d", remainingEOFs)
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs, sending EOFs")
			err = f.SendEndOfFile(clientID, englishFiltersAmount)
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}

			err := f.AckLastMessage()
			if err != nil {
				log.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize

			for k := range accumulatedRawReviewsMap[clientID] {
				delete(accumulatedRawReviewsMap[clientID], k)
			}
			for k := range gamesToSendMap[clientID] {
				delete(gamesToSendMap[clientID], k)
			}

			delete(accumulatedRawReviewsMap, clientID)
			delete(gamesToSendMap, clientID)
			delete(remainingEOFsMap, clientID)
		}

		if reviews != nil {
			log.Infof("Received review for client %d", clientID)
			err := f.handleRawReviews(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, reviews)
			if err != nil {
				log.Errorf("Failed to handle raw reviews: %v", err)
				return
			}
		}

		if gameReviewsMetrics != nil {
			log.Infof("Received game reviews metrics for client %d", clientID)
			err := f.handleGameReviewsMetrics(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, gameReviewsMetrics)
			if err != nil {
				log.Errorf("Failed to handle game reviews metrics: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			err = f.AckLastMessage()
			if err != nil {
				log.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = 50
		} else {
			messagesUntilAck--
		}
	}
}

func (f *NegativeReviewsPreFilter) handleRawReviews(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews map[int][]*r.RawReview, clientGamesToSend map[int]bool, rawReviews []*r.RawReview) error {
	for _, rawReview := range rawReviews {
		if shouldSend, exists := clientGamesToSend[int(rawReview.AppId)]; exists {
			if shouldSend && !rawReview.Positive {
				err := f.SendReview(clientId, englishFiltersAmount, rawReview)
				if err != nil {
					log.Errorf("Failed to send review: %v", err)
					return err
				}
				log.Infof("Sent review for client %d", clientId)
			} else {
				return nil
			}
		} else {
			if !rawReview.Positive {
				clientAccumulatedRawReviews[int(rawReview.AppId)] = append(clientAccumulatedRawReviews[int(rawReview.AppId)], rawReview)
				log.Infof("Accumulated review for client %d", clientId)
			}
		}
	}
	return nil
}

func (f *NegativeReviewsPreFilter) handleGameReviewsMetrics(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews map[int][]*r.RawReview, clientGamesToSend map[int]bool, gameReviewsMetrics []*reviews_accumulator.GameReviewsMetrics) error {
	for _, gameReviewsMetric := range gameReviewsMetrics {
		if gameReviewsMetric.NegativeReviews >= MinNegativeReviews {
			clientGamesToSend[int(gameReviewsMetric.AppID)] = true
			if reviews, exists := clientAccumulatedRawReviews[int(gameReviewsMetric.AppID)]; exists {
				for _, rawReview := range reviews {
					err := f.SendReview(clientId, englishFiltersAmount, rawReview)
					if err != nil {
						log.Errorf("Failed to send review: %v", err)
						return err
					}
					log.Infof("Sent review for client %d", clientId)
				}
				delete(clientAccumulatedRawReviews, int(gameReviewsMetric.AppID))
			}
		} else {
			clientGamesToSend[int(gameReviewsMetric.AppID)] = false
			delete(clientAccumulatedRawReviews, int(gameReviewsMetric.AppID))
		}
	}

	return nil
}
