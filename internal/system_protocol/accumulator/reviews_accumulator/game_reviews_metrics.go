package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"errors"
)

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

func (m *GameReviewsMetrics) UpdateWithReview(review *r.ReducedReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func (m *GameReviewsMetrics) UpdateWithRawReview(review *r.RawReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func SerializeGameReviewsMetrics(metrics *GameReviewsMetrics) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], metrics.AppID)
	binary.BigEndian.PutUint32(buf[4:8], uint32(metrics.PositiveReviews))
	binary.BigEndian.PutUint32(buf[8:12], uint32(metrics.NegativeReviews))
	return buf
}

func DeserializeGameReviewsMetrics(data []byte) (*GameReviewsMetrics, error) {
	if len(data) != 12 {
		return nil, errors.New("Data too short to deserialize into GameReviewsMetrics")
	}

	metrics := &GameReviewsMetrics{
		AppID:           binary.BigEndian.Uint32(data[0:4]),
		PositiveReviews: int(binary.BigEndian.Uint32(data[4:8])),
		NegativeReviews: int(binary.BigEndian.Uint32(data[8:12])),
	}
	return metrics, nil
}
