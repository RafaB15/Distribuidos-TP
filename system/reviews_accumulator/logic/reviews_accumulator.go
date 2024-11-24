package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReceiveReviewsFunc func() (clientID int, rawReviews []*reviews.RawReview, eof bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, accumulatedReviews map[uint32]*r.GameReviewsMetrics, indieReviewJoinersAmount int) error
type AckLastMessageFunc func() error
type SendEofFunc func(clientID int, senderID int, indieReviewJoinersAmount int) error

type ReviewsAccumulator struct {
	ReceiveReviews         ReceiveReviewsFunc
	SendAccumulatedReviews SendAccumulatedReviewsFunc
	AckLastMessage         AckLastMessageFunc
	SendEof                SendEofFunc
}

func NewReviewsAccumulator(
	receiveReviews ReceiveReviewsFunc,
	sendAccumulatedReviews SendAccumulatedReviewsFunc,
	ackLastMessage AckLastMessageFunc,
	sendEof SendEofFunc,
) *ReviewsAccumulator {
	return &ReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		AckLastMessage:         ackLastMessage,
		SendEof:                sendEof,
	}
}

func (ra *ReviewsAccumulator) Run(id int, indieReviewJoinersAmount int) {
	accumulatedReviews := make(map[int]map[uint32]*r.GameReviewsMetrics)

	messagesUntilAck := 100

	for {
		clientID, rawReviews, eof, err := ra.ReceiveReviews()
		if err != nil {
			log.Error("Error receiving reviews: ", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews[clientID]
		if !exists {
			clientAccumulatedReviews = make(map[uint32]*r.GameReviewsMetrics)
			accumulatedReviews[clientID] = clientAccumulatedReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)
			err = ra.SendAccumulatedReviews(clientID, clientAccumulatedReviews, indieReviewJoinersAmount)
			if err != nil {
				log.Errorf("error sending accumulated reviews: %s", err)
				return
			}
			log.Info("Sent accumulated reviews")

			err = ra.SendEof(clientID, id, indieReviewJoinersAmount)
			if err != nil {
				log.Errorf("error sending EOF: %s", err)
				return
			}
			log.Info("Sent EOFs")

			err := ra.AckLastMessage()
			if err != nil {
				log.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = 100

			delete(accumulatedReviews, clientID)
			continue
		}

		for _, review := range rawReviews {
			// log.Infof("Received review for app %d with review id %d", review.AppId, review.ReviewId)
			if metrics, exists := clientAccumulatedReviews[review.AppId]; exists {
				// log.Infof("Accumulating review for app %d", review.AppId)
				metrics.UpdateWithRawReview(review)
			} else {
				newMetrics := r.NewReviewsMetrics(review.AppId)
				newMetrics.UpdateWithRawReview(review)
				clientAccumulatedReviews[review.AppId] = newMetrics
			}
		}

		if messagesUntilAck == 0 {
			err = ra.AckLastMessage()
			if err != nil {
				log.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = 100
		} else {
			messagesUntilAck--
		}
	}
}
