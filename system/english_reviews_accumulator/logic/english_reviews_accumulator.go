package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type EnglishReviewsAccumulator struct {
	ReceiveReviews         func() ([]*r.Review, bool, error)
	SendAccumulatedReviews func([]*ra.GameReviewsMetrics) error
	SendEndOfFiles         func(int) error
}

func NewEnglishReviewsAccumulator(receiveReviews func() ([]*r.Review, bool, error), sendAccumulatedReviews func([]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int) error) *EnglishReviewsAccumulator {
	return &EnglishReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEndOfFiles:         sendEndOfFiles,
	}
}

func (a *EnglishReviewsAccumulator) Run(englishFiltersAmount int, positiveReviewsFiltersAmount int) {
	remainingEOFs := englishFiltersAmount

	accumulatedReviews := make(map[uint32]*ra.GameReviewsMetrics)

	for {
		reviews, eof, err := a.ReceiveReviews()
		if err != nil {
			log.Errorf("Failed to receive reviews: %v", err)
			return
		}

		if eof {
			remainingEOFs--
			log.Info("Received EOF")
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs")

			metrics := idMapToList(accumulatedReviews)
			err = a.SendAccumulatedReviews(metrics)
			if err != nil {
				log.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}

			err = a.SendEndOfFiles(positiveReviewsFiltersAmount)
			if err != nil {
				log.Errorf("Failed to send EOFs: %v", err)
				return
			}
			continue
		}

		for _, review := range reviews {
			if metrics, exists := accumulatedReviews[review.AppId]; exists {
				log.Info("Updating metrics for appID: ", review.AppId)
				// Update existing metrics
				metrics.UpdateWithReview(review)
			} else {
				// Create new metrics
				log.Info("Creating new metrics for appID: ", review.AppId)
				newMetrics := ra.NewReviewsMetrics(review.AppId)
				newMetrics.UpdateWithReview(review)
				accumulatedReviews[review.AppId] = newMetrics
			}
		}

	}
}

func idMapToList(idMap map[uint32]*ra.GameReviewsMetrics) []*ra.GameReviewsMetrics {
	var list []*ra.GameReviewsMetrics
	for _, value := range idMap {
		list = append(list, value)
	}
	return list
}
