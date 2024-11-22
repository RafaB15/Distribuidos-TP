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
	AckBatchSize       = 500
)

type ReceiveMessageFunc func(tracker *n.MessageTracker) (clientID int, reviews []*r.RawReview, gameReviewsMetrics []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, e error)
type SendReviewFunc func(clientID int, englishFiltersAmount int, review *r.RawReview, tracker *n.MessageTracker) error
type AckLastMessageFunc func() error
type SendEndOfFileFunc func(clientID int, senderID int, englishFiltersAmount int, tracker *n.MessageTracker) error

type NegativeReviewsPreFilter struct {
	ReceiveMessage ReceiveMessageFunc
	SendReview     SendReviewFunc
	AckLastMessage AckLastMessageFunc
	SendEndOfFile  SendEndOfFileFunc
	logger         *logging.Logger
}

func NewNegativeReviewsPreFilter(
	receiveMessage ReceiveMessageFunc,
	sendReview SendReviewFunc,
	ackLastMessage AckLastMessageFunc,
	sendEndOfFile SendEndOfFileFunc,
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

func (f *NegativeReviewsPreFilter) Run(id int, repository *p.Repository, englishFiltersAmount int, accumulatorsAmount int) {
	accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber, err := repository.LoadAll(accumulatorsAmount + 1)
	if err != nil {
		f.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, reviews, gameReviewsMetrics, eof, newMessage, err := f.ReceiveMessage(messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to receive message: %v", err)
			return
		}

		if newMessage && !eof {

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

			if reviews != nil {
				err := f.handleRawReviews(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, reviews, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle raw reviews: %v", err)
					return
				}
			}

			if gameReviewsMetrics != nil {
				f.logger.Infof("Received game reviews metrics for client %d", clientID)
				err := f.handleGameReviewsMetrics(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, gameReviewsMetrics, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle game reviews metrics: %v", err)
					return
				}
			}
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending EOFs")
			err = f.SendEndOfFile(clientID, id, englishFiltersAmount, messageTracker)
			if err != nil {
				f.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			accumulatedRawReviewsMap.Delete(clientID)
			gamesToSendMap.Delete(clientID)
			messageTracker.DeleteClientInfo(clientID) //Sería mejor borrar toda la info

			syncNumber++
			err := repository.SaveAll(accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber)
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
			err := repository.SaveAll(accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save data: %v", err)
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

func (f *NegativeReviewsPreFilter) handleRawReviews(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], rawReviews []*r.RawReview, messageTracker *n.MessageTracker) error {
	for _, rawReview := range rawReviews {
		if shouldSend, exists := clientGamesToSend.Get(int(rawReview.AppId)); exists {
			if shouldSend && !rawReview.Positive {
				err := f.SendReview(clientId, englishFiltersAmount, rawReview, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to send review: %v", err)
					return err
				}
				f.logger.Infof("Sent review for client %d", clientId)
			} else {
				continue
			}
		} else {
			if !rawReview.Positive {
				currentReviews, _ := clientAccumulatedRawReviews.Get(int(rawReview.AppId))
				clientAccumulatedRawReviews.Set(int(rawReview.AppId), append(currentReviews, rawReview))
			}
		}
	}
	return nil
}

func (f *NegativeReviewsPreFilter) handleGameReviewsMetrics(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], gameReviewsMetrics []*reviews_accumulator.GameReviewsMetrics, messageTracker *n.MessageTracker) error {
	for _, gameReviewsMetric := range gameReviewsMetrics {
		if gameReviewsMetric.NegativeReviews >= MinNegativeReviews {
			clientGamesToSend.Set(int(gameReviewsMetric.AppID), true)
			if reviews, exists := clientAccumulatedRawReviews.Get(int(gameReviewsMetric.AppID)); exists {
				for _, rawReview := range reviews {
					err := f.SendReview(clientId, englishFiltersAmount, rawReview, messageTracker)
					if err != nil {
						f.logger.Errorf("Failed to send review: %v", err)
						return err
					}
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
