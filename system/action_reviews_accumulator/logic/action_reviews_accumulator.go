package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 500
)

type ReceiveReviewFunc func(messageTracker *n.MessageTracker) (clientID int, reducedReview *r.ReducedReview, eof bool, newMessage bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, metrics []*ra.NamedGameReviewsMetrics) error
type SendEndOfFilesFunc func(clientID int) error
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

func (a *ActionReviewsAccumulator) Run(actionReviewJoinersAmount int) {
	messageTracker := n.NewMessageTracker(actionReviewJoinersAmount)
	messagesUntilAck := AckBatchSize

	accumulatedReviews := make(map[int]map[uint32]*ra.NamedGameReviewsMetrics)

	for {
		clientID, reducedReview, eof, newMessage, err := a.ReceiveReview(messageTracker)
		if err != nil {
			a.logger.Errorf("Failed to receive reviews: %v", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews[clientID]
		if !exists {
			clientAccumulatedReviews = make(map[uint32]*ra.NamedGameReviewsMetrics)
			accumulatedReviews[clientID] = clientAccumulatedReviews
		}

		if newMessage && !eof {
			if metrics, exists := clientAccumulatedReviews[reducedReview.AppId]; exists {
				// a.logger.Info("Updating metrics for appID: ", reducedReview.AppId)
				// Update existing metrics
				metrics.UpdateWithReview(reducedReview)
			} else {
				// Create new metrics
				//a.logger.Info("Creating new metrics for appID: ", reducedReview.AppId)
				newMetrics := ra.NewNamedGameReviewsMetrics(reducedReview.AppId, reducedReview.Name)
				newMetrics.UpdateWithReview(reducedReview)
				clientAccumulatedReviews[reducedReview.AppId] = newMetrics
			}
		}

		if messageTracker.ClientFinished(clientID, a.logger) {
			a.logger.Infof("Client %d finished", clientID)

			metrics := idMapToList(clientAccumulatedReviews)
			err = a.SendAccumulatedReviews(clientID, metrics)
			if err != nil {
				a.logger.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}

			a.logger.Info("Sending EOFs")
			err = a.SendEndOfFiles(clientID)
			if err != nil {
				a.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			messagesUntilAck = AckBatchSize
			err = a.AckLastMessage()
			if err != nil {
				a.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
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

func idMapToList(idMap map[uint32]*ra.NamedGameReviewsMetrics) []*ra.NamedGameReviewsMetrics {
	var list []*ra.NamedGameReviewsMetrics
	for _, value := range idMap {
		list = append(list, value)
	}
	return list
}
