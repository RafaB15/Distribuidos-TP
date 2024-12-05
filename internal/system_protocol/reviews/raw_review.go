package reviews

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"strings"
)

type RawReview struct {
	ReviewId   uint32
	AppId      uint32
	Positive   bool
	ReviewText string
}

func NewRawReview(reviewId uint32, appId uint32, positive bool, reviewText string) *RawReview {
	return &RawReview{
		ReviewId:   reviewId,
		AppId:      appId,
		Positive:   positive,
		ReviewText: reviewText,
	}
}

func NewRawReviewFromStrings(reviewId uint32, appId string, reviewScore string, reviewText string) (*RawReview, error) {
	appIdUint, err := strconv.ParseUint(appId, 10, 32)
	if err != nil {
		return nil, err
	}

	positive := reviewScore == "1"

	return &RawReview{
		ReviewId:   reviewId,
		AppId:      uint32(appIdUint),
		Positive:   positive,
		ReviewText: reviewText,
	}, nil
}

func (r *RawReview) Serialize() []byte {
	reviewIdSize := 4
	appIdSize := 4
	amountSize := 4
	boolSize := 1

	rawReviewSize := reviewIdSize + appIdSize + boolSize + amountSize + len(r.ReviewText)

	offset := 0

	buf := make([]byte, rawReviewSize)

	binary.LittleEndian.PutUint32(buf[offset:reviewIdSize], r.ReviewId)
	offset += reviewIdSize

	binary.LittleEndian.PutUint32(buf[offset:offset+appIdSize], r.AppId)
	offset += appIdSize

	if r.Positive {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	offset += boolSize

	amount := len(r.ReviewText)
	binary.LittleEndian.PutUint32(buf[offset:offset+amountSize], uint32(amount))
	offset += amountSize

	copy(buf[offset:], r.ReviewText)

	return buf
}

func DeserializeRawReview(buf []byte) (*RawReview, int, error) {
	reviewIdSize := 4
	appIdSize := 4
	amountSize := 4
	boolSize := 1

	if len(buf) < reviewIdSize+appIdSize+boolSize+amountSize {
		return nil, 0, errors.New("buffer too short")
	}

	offset := 0

	reviewId := binary.LittleEndian.Uint32(buf[offset : offset+reviewIdSize])
	offset += reviewIdSize

	appId := binary.LittleEndian.Uint32(buf[offset : offset+appIdSize])
	offset += appIdSize

	positive := buf[offset] == 1
	offset += boolSize

	amount := binary.LittleEndian.Uint32(buf[offset : offset+amountSize])
	offset += amountSize

	if len(buf) < offset+int(amount) {
		return nil, 0, errors.New("buffer too short for review text")
	}

	reviewText := string(buf[offset : offset+int(amount)])
	offset += int(amount)

	return &RawReview{
		ReviewId:   reviewId,
		AppId:      appId,
		Positive:   positive,
		ReviewText: reviewText,
	}, offset, nil
}

func DeserializeRawReviewsBatchFromStrings(reviews []string, appIdIndex int, reviewScoreIndex int, reviewTextIndex int, currentReviewId int) ([]*RawReview, error) {
	rawReviews := make([]*RawReview, 0)
	reviewId := currentReviewId

	for _, line := range reviews {
		reader := csv.NewReader(strings.NewReader(line))
		records, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		rawReview, err := NewRawReviewFromStrings(uint32(reviewId), records[appIdIndex], records[reviewScoreIndex], records[reviewTextIndex])
		reviewId++
		if err != nil {
			return nil, err
		}

		rawReviews = append(rawReviews, rawReview)
	}

	return rawReviews, nil
}

func SerializeRawReviewsBatch(reviews []*RawReview) []byte {
	var result []byte

	// Add the amount of reviews as a uint16 at the beginning
	reviewCount := uint16(len(reviews))
	countBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(countBytes, reviewCount)
	result = append(result, countBytes...)

	for _, review := range reviews {
		serializedReview := review.Serialize()
		result = append(result, serializedReview...)
	}

	return result
}

func DeserializeRawReviewsBatch(buf []byte) ([]*RawReview, error) {
	if len(buf) < 2 {
		return nil, errors.New("buffer too short")
	}

	reviewCount := binary.LittleEndian.Uint16(buf[:2])
	buf = buf[2:]

	rawReviews := make([]*RawReview, 0)

	for i := 0; i < int(reviewCount); i++ {
		review, amountRead, err := DeserializeRawReview(buf)
		if err != nil {
			return nil, err
		}

		rawReviews = append(rawReviews, review)
		buf = buf[amountRead:]
	}

	return rawReviews, nil
}
