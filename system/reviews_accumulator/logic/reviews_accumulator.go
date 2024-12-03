package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	p "distribuidos-tp/system/reviews_accumulator/persistence"

	n "distribuidos-tp/internal/system_protocol/node"
	"distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 100
	expectedEOFs = 1
)

type ReceiveReviewsFunc func(messageTracker *n.MessageTracker) (clientID int, rawReviews []*reviews.ReducedRawReview, eof bool, newMessage bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, accumulatedReviews *n.IntMap[*r.GameReviewsMetrics], indieReviewJoinersAmount int, messageTracker *n.MessageTracker) error
type AckLastMessageFunc func() error
type SendEofFunc func(clientID int, senderID int, indieReviewJoinersAmount int, messageTracker *n.MessageTracker) error

type ReviewsAccumulator struct {
	ReceiveReviews         ReceiveReviewsFunc
	SendAccumulatedReviews SendAccumulatedReviewsFunc
	AckLastMessage         AckLastMessageFunc
	SendEof                SendEofFunc
	logger                 *logging.Logger
}

func NewReviewsAccumulator(
	receiveReviews ReceiveReviewsFunc,
	sendAccumulatedReviews SendAccumulatedReviewsFunc,
	ackLastMessage AckLastMessageFunc,
	sendEof SendEofFunc,
	logger *logging.Logger,
) *ReviewsAccumulator {
	return &ReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		AckLastMessage:         ackLastMessage,
		SendEof:                sendEof,
		logger:                 logger,
	}
}

func (ra *ReviewsAccumulator) Run(id int, indieReviewJoinersAmount int, repository *p.Repository) {
	accumulatedReviewsMap, messageTracker, syncNumber, err := repository.LoadAll(expectedEOFs)
	if err != nil {
		ra.logger.Errorf("Failed to load data: %v", err)
		return
	}
	messagesUntilAck := AckBatchSize

	for {
		clientID, rawReviews, eof, newMessage, err := ra.ReceiveReviews(messageTracker)
		if err != nil {
			ra.logger.Error("Error receiving reviews: ", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviewsMap.Get(clientID)
		if !exists {
			clientAccumulatedReviews = repository.InitializeAccumulatedReviewsMap()
			accumulatedReviewsMap.Set(clientID, clientAccumulatedReviews)
		}

		if newMessage && !eof {
			ra.logger.Infof("Received reviews from client %d", clientID)

			for _, review := range rawReviews {
				// log.Infof("Received review for app %d with review id %d", review.AppId, review.ReviewId)
				if metrics, exists := clientAccumulatedReviews.Get(int(review.AppId)); exists {
					// log.Infof("Accumulating review for app %d", review.AppId)
					metrics.UpdateWithReducedRawReview(review)
				} else {
					newMetrics := r.NewReviewsMetrics(review.AppId)
					newMetrics.UpdateWithReducedRawReview(review)
					clientAccumulatedReviews.Set(int(review.AppId), newMetrics)
				}
			}
		}

		if messageTracker.ClientFinished(clientID, ra.logger) {
			ra.logger.Infof("Received all EOFs of client %d. Sending accumulated reviews", clientID)
			err = ra.SendAccumulatedReviews(clientID, clientAccumulatedReviews, indieReviewJoinersAmount, messageTracker)
			if err != nil {
				ra.logger.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}
			ra.logger.Infof("Sent accumulated reviews")

			err = ra.SendEof(clientID, id, indieReviewJoinersAmount, messageTracker)
			if err != nil {
				ra.logger.Errorf("Failed to send EOF: %v", err)
				return
			}
			ra.logger.Infof("Sent EOFs of client %d", clientID)

			messageTracker.DeleteClientInfo(clientID)

			syncNumber++
			err = repository.SaveAll(accumulatedReviewsMap, messageTracker, syncNumber)
			if err != nil {
				ra.logger.Errorf("Failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = ra.AckLastMessage()
			if err != nil {
				ra.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			syncNumber++
			err = repository.SaveAll(accumulatedReviewsMap, messageTracker, syncNumber)
			if err != nil {
				ra.logger.Errorf("Failed to save data: %v", err)
				return
			}

			err = ra.AckLastMessage()
			if err != nil {
				ra.logger.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}
