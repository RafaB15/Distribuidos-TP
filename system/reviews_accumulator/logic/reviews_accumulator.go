package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"

	"distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReviewsAccumulator struct {
	ReceiveReviews         func() ([]*reviews.Review, bool, error)
	SendAccumulatedReviews func(map[uint32]*r.GameReviewsMetrics) error
	SendEof                func() error
}

func NewReviewsAccumulator(receiveReviews func() ([]*reviews.Review, bool, error), sendAccumulatedReviews func(map[uint32]*r.GameReviewsMetrics) error, sendEof func() error) *ReviewsAccumulator {
	return &ReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEof:                sendEof,
	}
}

func (ra *ReviewsAccumulator) Run(accumulatorsAmount int) {
	remainingEOFs := accumulatorsAmount

	accumulatedReviews := make(map[uint32]*r.GameReviewsMetrics)

	for {
		reviews, eof, err := ra.ReceiveReviews()
		if err != nil {
			log.Error("Error receiving reviews: ", err)
			return
		}

		if eof {
			log.Info("Received EOF")
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}

			err = ra.SendAccumulatedReviews(accumulatedReviews)
			if err != nil {
				log.Errorf("Error sending accumulated reviews: ", err)
				return
			}
			log.Info("Sent accumulated reviews")

			err = ra.SendEof()
			if err != nil {
				log.Errorf("Error sending EOF: ", err)
			}
			log.Info("Sent EOFs")
		}

		for _, review := range reviews {
			if metrics, exists := accumulatedReviews[review.AppId]; exists {
				log.Infof("Accumulating review for app %d", review.AppId)
				metrics.UpdateWithReview(review)
			} else {
				log.Infof("Creating metrics for app %d", review.AppId)
				newMetrics := r.NewReviewsMetrics(review.AppId)
				newMetrics.UpdateWithReview(review)
				accumulatedReviews[review.AppId] = newMetrics
			}
		}
	}
}
