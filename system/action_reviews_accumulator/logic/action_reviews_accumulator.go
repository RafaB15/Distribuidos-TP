package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	p "distribuidos-tp/system/action_reviews_accumulator/persistence"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 200
)

type ReceiveReviewFunc func(messageTracker *n.MessageTracker) (clientID int, reducedReview *r.ReducedReview, eof bool, newMessage bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, metrics []*ra.NamedGameReviewsMetrics, messageTracker *n.MessageTracker) error
type SendEndOfFilesFunc func(clientID int, messageTracker *n.MessageTracker) error
type AckLastMessageFunc func() error

type ActionReviewsAccumulator struct {
	ReceiveReview          ReceiveReviewFunc
	SendAccumulatedReviews SendAccumulatedReviewsFunc
	SendEndOfFiles         SendEndOfFilesFunc
	AckLastMessage         AckLastMessageFunc
	logger                 *logging.Logger
}

func NewActionReviewsAccumulator(
	receiveReview ReceiveReviewFunc,
	sendAccumulatedReviews SendAccumulatedReviewsFunc,
	sendEndOfFiles SendEndOfFilesFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *ActionReviewsAccumulator {
	return &ActionReviewsAccumulator{
		ReceiveReview:          receiveReview,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEndOfFiles:         sendEndOfFiles,
		AckLastMessage:         ackLastMessage,
		logger:                 logger,
	}
}

func (a *ActionReviewsAccumulator) Run(actionReviewJoinersAmount int, repository *p.Repository) {
	accumulatedReviews, messageTracker, syncNumber, err := repository.LoadAll(actionReviewJoinersAmount)
	if err != nil {
		a.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, reducedReview, eof, newMessage, err := a.ReceiveReview(messageTracker)
		if err != nil {
			a.logger.Errorf("Failed to receive reviews: %v", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews.Get(clientID)
		if !exists {
			clientAccumulatedReviews = repository.InitializeAccumulatedReviewsMap()
			accumulatedReviews.Set(clientID, clientAccumulatedReviews)
		}

		if newMessage && !eof {
			if metrics, exists := clientAccumulatedReviews.Get(int(reducedReview.AppId)); exists {
				// a.logger.Info("Updating metrics for appID: ", reducedReview.AppId)
				// Update existing metrics
				metrics.UpdateWithReview(reducedReview)
			} else {
				// Create new metrics
				//a.logger.Info("Creating new metrics for appID: ", reducedReview.AppId)
				newMetrics := ra.NewNamedGameReviewsMetrics(reducedReview.AppId, reducedReview.Name)
				newMetrics.UpdateWithReview(reducedReview)
				clientAccumulatedReviews.Set(int(reducedReview.AppId), newMetrics)
			}
		}

		if messageTracker.ClientFinished(clientID, a.logger) {
			a.logger.Infof("Client %d finished", clientID)

			metrics := clientAccumulatedReviews.Values()
			err = a.SendAccumulatedReviews(clientID, metrics, messageTracker)
			if err != nil {
				a.logger.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}

			a.logger.Info("Sending EOFs")
			err = a.SendEndOfFiles(clientID, messageTracker)
			if err != nil {
				a.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			syncNumber++
			err = repository.SaveAll(accumulatedReviews, messageTracker, syncNumber)
			if err != nil {
				a.logger.Errorf("Failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = a.AckLastMessage()
			if err != nil {
				a.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			syncNumber++
			err = repository.SaveAll(accumulatedReviews, messageTracker, syncNumber)
			if err != nil {
				a.logger.Errorf("Failed to save data: %v", err)
				return
			}

			err = a.AckLastMessage()
			if err != nil {
				a.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}
