package reviews

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type ReducedRawReview struct {
	ReviewId uint32
	AppId    uint32
	Positive bool
}

func NewReducedRawReview(reviewId uint32, appId uint32, positive bool) *ReducedRawReview {
	return &ReducedRawReview{
		ReviewId: reviewId,
		AppId:    appId,
		Positive: positive,
	}
}

func (r *ReducedRawReview) Serialize() []byte {
	buf := make([]byte, 9)
	binary.LittleEndian.PutUint32(buf[:4], r.ReviewId)
	binary.LittleEndian.PutUint32(buf[4:8], r.AppId)
	if r.Positive {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	return buf
}

func DeserializeReducedRawReview(data []byte) (*ReducedRawReview, error) {
	if len(data) != 9 {
		return nil, fmt.Errorf("invalid data length: %d", len(data))
	}

	reviewId := binary.LittleEndian.Uint32(data[:4])
	appId := binary.LittleEndian.Uint32(data[4:8])
	positive := data[8] == 1

	return &ReducedRawReview{
		ReviewId: reviewId,
		AppId:    appId,
		Positive: positive,
	}, nil
}

func SerializeReducedRawReviewsBatch(reducedRawReviews []*ReducedRawReview) []byte {
	var result []byte

	// Add the amount of reducedRawReviews as a uint16 at the beginning
	reviewCount := uint16(len(reducedRawReviews))
	countBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(countBytes, reviewCount)
	result = append(result, countBytes...)

	for _, review := range reducedRawReviews {
		serializedReview := review.Serialize()
		result = append(result, serializedReview...)
	}

	return result
}

func DeserializeReducedRawReviewsBatch(buf []byte) ([]*ReducedRawReview, error) {
	if len(buf) < 2 {
		return nil, errors.New("buffer too short")
	}

	reviewCount := binary.LittleEndian.Uint16(buf[:2])

	offset := 2

	reducedReviews := make([]*ReducedRawReview, 0)

	for i := 0; i < int(reviewCount); i++ {
		reducedReview, err := DeserializeReducedRawReview(buf[offset : offset+9])
		if err != nil {
			return nil, err
		}

		reducedReviews = append(reducedReviews, reducedReview)
		offset += 9
	}

	return reducedReviews, nil
}
