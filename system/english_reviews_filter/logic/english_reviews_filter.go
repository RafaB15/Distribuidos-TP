package english_reviews_filter

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	"github.com/op/go-logging"
)

type EnglishReviewsFilter struct {
	ReceiveGameReviews func() (int, *r.RawReview, bool, error)
	SendEnglishReview  func(clientID int, review *r.Review, englishAccumulatorsAmount int) error
	SendEnfOfFiles     func(clientID int, accumulatorsAmount int) error
	Logger             *logging.Logger
}

func NewEnglishReviewsFilter(
	receiveGameReviews func() (int, *r.RawReview, bool, error),
	sendEnglishReviews func(int, *r.Review, int) error,
	sendEndOfFiles func(int, int) error,
	logger *logging.Logger,
) *EnglishReviewsFilter {
	return &EnglishReviewsFilter{
		ReceiveGameReviews: receiveGameReviews,
		SendEnglishReview:  sendEnglishReviews,
		SendEnfOfFiles:     sendEndOfFiles,
		Logger:             logger,
	}
}

func (f *EnglishReviewsFilter) Run(accumulatorsAmount int, negativeReviewsPreFilterAmount int) {
	remainingEOFsMap := make(map[int]int)
	languageIdentifier := r.NewLanguageIdentifier()

	for {
		clientID, rawReview, eof, err := f.ReceiveGameReviews()
		if err != nil {
			f.Logger.Errorf("Failed to receive game review: %v", err)
			return
		}

		if eof {
			f.Logger.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = negativeReviewsPreFilterAmount
			}
			f.Logger.Infof("Remaining EOFs: %d", remainingEOFs)
			remainingEOFs--
			f.Logger.Infof("Remaining EOFs AFTER: %d", remainingEOFs)
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			f.Logger.Info("Received all EOFs, sending EOFs")
			err = f.SendEnfOfFiles(clientID, accumulatorsAmount)
			if err != nil {
				f.Logger.Errorf("Failed to send EOF: %v", err)
				return
			}
			delete(remainingEOFsMap, clientID)
			continue
		}

		f.Logger.Debugf("Before entering if statement")
		if languageIdentifier.IsEnglish(rawReview.ReviewText) {
			f.Logger.Infof("About to check if the review is in english")
			review := r.NewReview(rawReview.AppId, rawReview.Positive)
			err := f.SendEnglishReview(clientID, review, accumulatorsAmount)
			if err != nil {
				f.Logger.Errorf("Failed to send english review: %v", err)
				return
			}
			f.Logger.Infof("Sent english review for app %d", rawReview.AppId)
		} else {
			f.Logger.Infof("Review for app %d is not in english", rawReview.AppId)
		}
	}
}
