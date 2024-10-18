package english_reviews_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	r "distribuidos-tp/internal/system_protocol/reviews"
)

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
