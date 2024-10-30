package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type EnglishReviewsAccumulator struct {
	ReceiveReviews         func() (int, []*r.Review, bool, error)
	SendAccumulatedReviews func(int, []*ra.GameReviewsMetrics) error
	SendEndOfFiles         func(int, int) error
}

func NewEnglishReviewsAccumulator(receiveReviews func() (int, []*r.Review, bool, error), sendAccumulatedReviews func(int, []*ra.GameReviewsMetrics) error, sendEndOfFiles func(int, int) error) *EnglishReviewsAccumulator {
	return &EnglishReviewsAccumulator{
		ReceiveReviews:         receiveReviews,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEndOfFiles:         sendEndOfFiles,
	}
}

func (a *EnglishReviewsAccumulator) Run(englishFiltersAmount int, negativeReviewsFiltersAmount int) {
	remainingEOFsMap := make(map[int]int)

	accumulatedReviews := make(map[int]map[uint32]*ra.GameReviewsMetrics)

	for {
		clientID, reviews, eof, err := a.ReceiveReviews()
		if err != nil {
			log.Errorf("Failed to receive reviews: %v", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews[clientID]
		if !exists {
			clientAccumulatedReviews = make(map[uint32]*ra.GameReviewsMetrics)
			accumulatedReviews[clientID] = clientAccumulatedReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)
			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = englishFiltersAmount
			}
			log.Info("Remaining EOFs: ", remainingEOFs)
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			log.Info("Remaining EOFs AFTER: ", remainingEOFs)

			log.Infof("Received EOF for client %d. Remaining EOFs: %d", clientID, remainingEOFs)
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs")

			metrics := idMapToList(clientAccumulatedReviews)
			err = a.SendAccumulatedReviews(clientID, metrics)
			if err != nil {
				log.Errorf("Failed to send accumulated reviews: %v", err)
				return
			}

			err = a.SendEndOfFiles(clientID, negativeReviewsFiltersAmount)
			if err != nil {
				log.Errorf("Failed to send EOFs: %v", err)
				return
			}
			delete(accumulatedReviews, clientID)
			delete(remainingEOFsMap, clientID)
			continue
		}

		for _, review := range reviews {
			if metrics, exists := clientAccumulatedReviews[review.AppId]; exists {
				log.Info("Updating metrics for appID: ", review.AppId)
				// Update existing metrics
				metrics.UpdateWithReview(review)
			} else {
				// Create new metrics
				log.Info("Creating new metrics for appID: ", review.AppId)
				newMetrics := ra.NewReviewsMetrics(review.AppId)
				newMetrics.UpdateWithReview(review)
				clientAccumulatedReviews[review.AppId] = newMetrics
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
