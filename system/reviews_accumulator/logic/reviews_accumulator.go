package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReviewsAccumulator struct {
	ReceiveReviews         func() (int, []*reviews.Review, bool, error)
	SendAccumulatedReviews func(int, map[uint32]*r.GameReviewsMetrics, int, int) error
	SendEof                func(int, int, int) error
}

func NewReviewsAccumulator(receiveReviews func() (int, []*reviews.Review, bool, error), sendAccumulatedReviews func(int, map[uint32]*r.GameReviewsMetrics, int, int) error, sendEof func(int, int, int) error) *ReviewsAccumulator {
	return &ReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEof:                sendEof,
	}
}

func (ra *ReviewsAccumulator) Run(accumulatorsAmount int, indieReviewJoinersAmount int, negativeReviewPreFiltersAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedReviews := make(map[int]map[uint32]*r.GameReviewsMetrics)

	for {
		clientID, reviews, eof, err := ra.ReceiveReviews()
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
			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = accumulatorsAmount
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}

			err = ra.SendAccumulatedReviews(clientID, clientAccumulatedReviews, indieReviewJoinersAmount, negativeReviewPreFiltersAmount)
			if err != nil {
				log.Errorf("error sending accumulated reviews: %s", err)
				return
			}
			log.Info("Sent accumulated reviews")

			err = ra.SendEof(clientID, indieReviewJoinersAmount, negativeReviewPreFiltersAmount)
			if err != nil {
				log.Errorf("error sending EOF: %s", err)
			}
			log.Info("Sent EOFs")

			delete(accumulatedReviews, clientID)
			delete(remainingEOFsMap, clientID)
			continue
		}

		for _, review := range reviews {
			if metrics, exists := clientAccumulatedReviews[review.AppId]; exists {
				log.Infof("Accumulating review for app %d", review.AppId)
				metrics.UpdateWithReview(review)
			} else {
				log.Infof("Creating metrics for app %d", review.AppId)
				newMetrics := r.NewReviewsMetrics(review.AppId)
				newMetrics.UpdateWithReview(review)
				clientAccumulatedReviews[review.AppId] = newMetrics
			}
		}
	}
}
