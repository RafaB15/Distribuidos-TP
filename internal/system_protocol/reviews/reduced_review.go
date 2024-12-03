package reviews

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type ReducedReview struct {
	ReviewId uint32
	AppId    uint32
	Name     string
	Positive bool
}

func NewReducedReview(reviewId uint32, appId uint32, name string, positive bool) *ReducedReview {
	return &ReducedReview{
		ReviewId: reviewId,
		AppId:    appId,
		Name:     name,
		Positive: positive,
	}
}

func (r *ReducedReview) Serialize() []byte {
	reviewIdSize := 4
	appIdSize := 4
	boolSize := 1
	nameSize := len(r.Name)
	totalSize := reviewIdSize + appIdSize + boolSize + 2 + nameSize

	buf := make([]byte, totalSize)
	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:offset+reviewIdSize], r.ReviewId)
	offset += reviewIdSize

	binary.LittleEndian.PutUint32(buf[offset:offset+appIdSize], r.AppId)
	offset += appIdSize

	binary.LittleEndian.PutUint16(buf[offset:offset+4], uint16(nameSize))
	offset += 2

	copy(buf[offset:offset+nameSize], r.Name)
	offset += nameSize

	if r.Positive {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	return buf
}

func DeserializeReducedReview(data []byte) (*ReducedReview, int, error) {
	reviewIdSize := 4
	appIdSize := 4

	if len(data) < 11 {
		return nil, 0, fmt.Errorf("invalid data length: %d", len(data))
	}

	offset := 0

	reviewId := binary.LittleEndian.Uint32(data[offset : offset+reviewIdSize])
	offset += reviewIdSize

	appId := binary.LittleEndian.Uint32(data[offset : offset+appIdSize])
	offset += appIdSize

	nameSize := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	if len(data[offset:]) < int(nameSize) {
		return nil, 0, fmt.Errorf("invalid data length for name: %d", len(data))
	}

	name := string(data[offset : offset+int(nameSize)])
	offset += int(nameSize)

	positive := data[offset] == 1
	offset++

	return &ReducedReview{
		ReviewId: reviewId,
		AppId:    appId,
		Name:     name,
		Positive: positive,
	}, offset, nil
}

func SerializeReducedReviewsBatch(reducedReviews []*ReducedReview) []byte {
	var result []byte

	// Add the amount of reducedReviews as a uint16 at the beginning
	reviewCount := uint16(len(reducedReviews))
	countBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(countBytes, reviewCount)
	result = append(result, countBytes...)

	for _, review := range reducedReviews {
		serializedReview := review.Serialize()
		result = append(result, serializedReview...)
	}

	return result
}

func DeserializeReducedReviewsBatch(buf []byte) ([]*ReducedReview, error) {
	if len(buf) < 2 {
		return nil, errors.New("buffer too short")
	}

	reviewCount := binary.LittleEndian.Uint16(buf[:2])
	buf = buf[2:]

	reducedReviews := make([]*ReducedReview, 0)

	for i := 0; i < int(reviewCount); i++ {
		reducedReview, amountRead, err := DeserializeReducedReview(buf)
		if err != nil {
			return nil, err
		}

		reducedReviews = append(reducedReviews, reducedReview)
		buf = buf[amountRead:]
	}

	return reducedReviews, nil
}
