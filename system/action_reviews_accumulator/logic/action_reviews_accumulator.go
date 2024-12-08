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

type ReceiveReviewFunc func(messageTracker *n.MessageTracker) (clientID int, reducedReview *r.ReducedReview, eof bool, newMessage bool, delMessage bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, metrics []*ra.NamedGameReviewsMetrics, messageTracker *n.MessageTracker) error
type SendEndOfFilesFunc func(clientID int, senderID int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int) error
type AckLastMessageFunc func() error

type ActionReviewsAccumulator struct {
	ReceiveReview          ReceiveReviewFunc
	SendAccumulatedReviews SendAccumulatedReviewsFunc
	SendEndOfFiles         SendEndOfFilesFunc
	SendDeleteClient       SendDeleteClientFunc
	AckLastMessage         AckLastMessageFunc
	logger                 *logging.Logger
}

func NewActionReviewsAccumulator(
	receiveReview ReceiveReviewFunc,
	sendAccumulatedReviews SendAccumulatedReviewsFunc,
	sendEndOfFiles SendEndOfFilesFunc,
	sendDeleteClient SendDeleteClientFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *ActionReviewsAccumulator {
	return &ActionReviewsAccumulator{
		ReceiveReview:          receiveReview,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEndOfFiles:         sendEndOfFiles,
		SendDeleteClient:       sendDeleteClient,
		AckLastMessage:         ackLastMessage,
		logger:                 logger,
	}
}

func (a *ActionReviewsAccumulator) Run(id int, actionReviewJoinersAmount int, repository *p.Repository) {
	accumulatedReviews, messageTracker, syncNumber, err := repository.LoadAll(actionReviewJoinersAmount)
	if err != nil {
		a.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, reducedReview, eof, newMessage, delMessage, err := a.ReceiveReview(messageTracker)
		if err != nil {
			a.logger.Errorf("Failed to receive reviews: %v", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews.Get(clientID)
		if !exists {
			clientAccumulatedReviews = repository.InitializeAccumulatedReviewsMap()
			accumulatedReviews.Set(clientID, clientAccumulatedReviews)
		}

		if newMessage && !eof && !delMessage {
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

		if delMessage {
			a.logger.Infof("Deleting client %d information", clientID)
			err = a.SendDeleteClient(clientID)

			if err != nil {
				a.logger.Errorf("Failed to send delete client: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
			accumulatedReviews.Delete(clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, a.logger)
		if clientFinished {
			a.logger.Infof("Client %d finished", clientID)

			metrics := clientAccumulatedReviews.Values()
			err = a.SendAccumulatedReviews(clientID, metrics, messageTracker)
			if err != nil {
				a.logger.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}

			a.logger.Info("Sending EOFs")
			err = a.SendEndOfFiles(clientID, id, messageTracker)
			if err != nil {
				a.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
			accumulatedReviews.Delete(clientID)
		}

		if messagesUntilAck == 0 || delMessage || clientFinished {
			saves := 1
			if delMessage || clientFinished {
				saves = 2
			}

			for i := 0; i < saves; i++ {
				syncNumber++
				err = repository.SaveAll(accumulatedReviews, messageTracker, syncNumber)
				if err != nil {
					a.logger.Errorf("Failed to save data: %v", err)
					return
				}
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
