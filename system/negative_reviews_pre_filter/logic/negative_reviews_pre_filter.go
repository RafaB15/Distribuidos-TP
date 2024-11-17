package negative_reviews_pre_filter

import (
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/negative_reviews_pre_filter/persistence"

	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"github.com/op/go-logging"
)

const (
	MinNegativeReviews = 5000
	AckBatchSize       = 150
)

type NegativeReviewsPreFilter struct {
	ReceiveMessage func() (int, []*r.RawReview, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendReview     func(int, int, *r.RawReview) error
	AckLastMessage func() error
	SendEndOfFile  func(int, int) error
	logger         *logging.Logger
}

func NewNegativeReviewsPreFilter(
	receiveMessage func() (int, []*r.RawReview, []*reviews_accumulator.GameReviewsMetrics, bool, error),
	sendReview func(int, int, *r.RawReview) error,
	ackLastMessage func() error,
	sendEndOfFile func(int, int) error,
	logger *logging.Logger,
) *NegativeReviewsPreFilter {
	return &NegativeReviewsPreFilter{
		ReceiveMessage: receiveMessage,
		SendReview:     sendReview,
		AckLastMessage: ackLastMessage,
		SendEndOfFile:  sendEndOfFile,
		logger:         logger,
	}
}

func (f *NegativeReviewsPreFilter) Run(repository *p.Repository, englishFiltersAmount int, accumulatorsAmount int) {
	remainingEOFsMap := make(map[int]int)

	accumulatedRawReviewsMap := repository.LoadAccumulatedRawReviews()

	gamesToSendMap := repository.LoadGamesToSend()

	//accumulatedRawReviewsMap := make(map[int]map[int][]*r.RawReview)
	//gamesToSendMap := make(map[int]map[int]bool)

	messagesUntilAck := AckBatchSize

	for {
		clientID, reviews, gameReviewsMetrics, eof, err := f.ReceiveMessage()
		if err != nil {
			f.logger.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedRawReviews, exists := accumulatedRawReviewsMap.Get(clientID)
		if !exists {
			clientAccumulatedRawReviews = repository.InitializeRawReviewMap()
			accumulatedRawReviewsMap.Set(clientID, clientAccumulatedRawReviews)
		}

		clientGamesToSend, exists := gamesToSendMap.Get(clientID)
		if !exists {
			clientGamesToSend = repository.InitializeGamesToSendMap()
			gamesToSendMap.Set(clientID, clientGamesToSend)
		}

		if eof {
			f.logger.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = accumulatorsAmount + 1
			}
			f.logger.Infof("Remaining EOFs: %d", remainingEOFs)
			remainingEOFs--
			f.logger.Infof("Remaining EOFs AFTER: %d", remainingEOFs)
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			f.logger.Info("Received all EOFs, sending EOFs")
			err = f.SendEndOfFile(clientID, englishFiltersAmount)
			if err != nil {
				f.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			err := f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize

			/* Lo dejo para acordarme que si hay problemas de memoria puedo intentar emular esto
			for k := range accumulatedRawReviewsMap[clientID] {
				delete(accumulatedRawReviewsMap[clientID], k)
			}
			for k := range gamesToSendMap[clientID] {
				delete(gamesToSendMap[clientID], k)
			}*/

			accumulatedRawReviewsMap.Delete(clientID)
			gamesToSendMap.Delete(clientID)
			delete(remainingEOFsMap, clientID)
		}

		if reviews != nil {
			f.logger.Infof("Received review for client %d", clientID)
			err := f.handleRawReviews(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, reviews)
			if err != nil {
				f.logger.Errorf("Failed to handle raw reviews: %v", err)
				return
			}
		}

		if gameReviewsMetrics != nil {
			f.logger.Infof("Received game reviews metrics for client %d", clientID)
			err := f.handleGameReviewsMetrics(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, gameReviewsMetrics)
			if err != nil {
				f.logger.Errorf("Failed to handle game reviews metrics: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			err := repository.SaveAccumulatedRawReviews(accumulatedRawReviewsMap)
			if err != nil {
				f.logger.Errorf("Failed to save accumulated raw reviews: %v", err)
				return
			}

			err = repository.SaveGamesToSend(gamesToSendMap)
			if err != nil {
				f.logger.Errorf("Failed to save games to send: %v", err)
				return
			}

			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}

func (f *NegativeReviewsPreFilter) handleRawReviews(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], rawReviews []*r.RawReview) error {
	for _, rawReview := range rawReviews {
		if shouldSend, exists := clientGamesToSend.Get(int(rawReview.AppId)); exists {
			if shouldSend && !rawReview.Positive {
				err := f.SendReview(clientId, englishFiltersAmount, rawReview)
				if err != nil {
					f.logger.Errorf("Failed to send review: %v", err)
					return err
				}
				f.logger.Infof("Sent review for client %d", clientId)
			} else {
				return nil
			}
		} else {
			if !rawReview.Positive {
				currentReviews, _ := clientAccumulatedRawReviews.Get(int(rawReview.AppId))
				clientAccumulatedRawReviews.Set(int(rawReview.AppId), append(currentReviews, rawReview))
				f.logger.Infof("Accumulated review for client %d", clientId)
			}
		}
	}
	return nil
}

func (f *NegativeReviewsPreFilter) handleGameReviewsMetrics(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], gameReviewsMetrics []*reviews_accumulator.GameReviewsMetrics) error {
	for _, gameReviewsMetric := range gameReviewsMetrics {
		if gameReviewsMetric.NegativeReviews >= MinNegativeReviews {
			clientGamesToSend.Set(int(gameReviewsMetric.AppID), true)
			if reviews, exists := clientAccumulatedRawReviews.Get(int(gameReviewsMetric.AppID)); exists {
				for _, rawReview := range reviews {
					err := f.SendReview(clientId, englishFiltersAmount, rawReview)
					if err != nil {
						f.logger.Errorf("Failed to send review: %v", err)
						return err
					}
					f.logger.Infof("Sent review for client %d", clientId)
				}
				clientAccumulatedRawReviews.Delete(int(gameReviewsMetric.AppID))
			}
		} else {
			clientGamesToSend.Set(int(gameReviewsMetric.AppID), false)
			clientAccumulatedRawReviews.Delete(int(gameReviewsMetric.AppID))
		}
	}

	return nil
}
