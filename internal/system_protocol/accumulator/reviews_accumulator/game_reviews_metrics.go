package reviews_accumulator

import r "distribuidos-tp/internal/system_protocol/reviews"

type GameReviewsMetrics struct {
	AppID           uint32
	PositiveReviews int
	NegativeReviews int
}

func NewReviewsMetrics(appId uint32) *GameReviewsMetrics {

	return &GameReviewsMetrics{
		AppID:           appId,
		PositiveReviews: 0,
		NegativeReviews: 0,
	}
}

func (m *GameReviewsMetrics) UpdateWithReview(review *r.Review) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}
