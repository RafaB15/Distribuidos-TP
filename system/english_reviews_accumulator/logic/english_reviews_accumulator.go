package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReceiveReviewFunc func() (clientID int, reducedReview *r.ReducedReview, eof bool, e error)
type SendAccumulatedReviewsFunc func(clientID int, metrics []*ra.NamedGameReviewsMetrics) error
type SendEndOfFilesFunc func(clientID int) error

type EnglishReviewsAccumulator struct {
	ReceiveReview          ReceiveReviewFunc
	SendAccumulatedReviews SendAccumulatedReviewsFunc
	SendEndOfFiles         SendEndOfFilesFunc
}

func NewEnglishReviewsAccumulator(
	receiveReview ReceiveReviewFunc,
	sendAccumulatedReviews SendAccumulatedReviewsFunc,
	sendEndOfFiles SendEndOfFilesFunc,
) *EnglishReviewsAccumulator {
	return &EnglishReviewsAccumulator{
		ReceiveReview:          receiveReview,
		SendAccumulatedReviews: sendAccumulatedReviews,
		SendEndOfFiles:         sendEndOfFiles,
	}
}

func (a *EnglishReviewsAccumulator) Run(englishFiltersAmount int) {
	remainingEOFsMap := make(map[int]int)

	accumulatedReviews := make(map[int]map[uint32]*ra.NamedGameReviewsMetrics)

	for {
		clientID, reducedReview, eof, err := a.ReceiveReview()
		if err != nil {
			log.Errorf("Failed to receive reviews: %v", err)
			return
		}

		clientAccumulatedReviews, exists := accumulatedReviews[clientID]
		if !exists {
			clientAccumulatedReviews = make(map[uint32]*ra.NamedGameReviewsMetrics)
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

			err = a.SendEndOfFiles(clientID)
			if err != nil {
				log.Errorf("Failed to send EOFs: %v", err)
				return
			}
			delete(accumulatedReviews, clientID)
			delete(remainingEOFsMap, clientID)
			continue
		}

		if metrics, exists := clientAccumulatedReviews[reducedReview.AppId]; exists {
			log.Info("Updating metrics for appID: ", reducedReview.AppId)
			// Update existing metrics
			metrics.UpdateWithReview(reducedReview)
		} else {
			// Create new metrics
			log.Info("Creating new metrics for appID: ", reducedReview.AppId)
			newMetrics := ra.NewNamedGameReviewsMetrics(reducedReview.AppId, reducedReview.Name)
			newMetrics.UpdateWithReview(reducedReview)
			clientAccumulatedReviews[reducedReview.AppId] = newMetrics
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
