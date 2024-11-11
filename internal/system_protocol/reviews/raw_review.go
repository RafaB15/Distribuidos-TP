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
	AppId      uint32
	Positive   bool
	ReviewText string
}

func NewRawReview(appId uint32, positive bool, reviewText string) *RawReview {
	return &RawReview{
		AppId:      appId,
		Positive:   positive,
		ReviewText: reviewText,
	}
}

func NewRawReviewFromStrings(appId string, reviewScore string, reviewText string) (*RawReview, error) {
	appIdUint, err := strconv.ParseUint(appId, 10, 32)
	if err != nil {
		return nil, err
	}

	positive := reviewScore == "1"

	return &RawReview{
		AppId:      uint32(appIdUint),
		Positive:   positive,
		ReviewText: reviewText,
	}, nil
}

func (r *RawReview) Serialize() []byte {
	appIdSize := 4
	amountSize := 4
	boolSize := 1

	rawReviewSize := appIdSize + boolSize + amountSize + len(r.ReviewText)

	buf := make([]byte, rawReviewSize)
	binary.LittleEndian.PutUint32(buf[:appIdSize], r.AppId)
	if r.Positive {
		buf[4] = 1
	} else {
		buf[4] = 0
	}

	amount := len(r.ReviewText)
	binary.LittleEndian.PutUint32(buf[appIdSize+boolSize:appIdSize+boolSize+amountSize], uint32(amount))
	copy(buf[appIdSize+boolSize+amountSize:], []byte(r.ReviewText))

	return buf
}

func DeserializeRawReview(buf []byte) (*RawReview, int, error) {
	appIdSize := 4
	amountSize := 4
	boolSize := 1

	if len(buf) < appIdSize+boolSize+amountSize {
		return nil, 0, errors.New("buffer too short")
	}

	appId := binary.LittleEndian.Uint32(buf[:appIdSize])
	positive := buf[4] == 1
	amount := binary.LittleEndian.Uint32(buf[appIdSize+boolSize : appIdSize+boolSize+amountSize])

	if len(buf) < int(appIdSize+boolSize+amountSize+int(amount)) {
		return nil, 0, errors.New("buffer too short for review text")
	}

	reviewText := string(buf[9 : 9+amount])

	totalRead := appIdSize + amountSize + boolSize + int(amount)

	return &RawReview{
		AppId:      appId,
		Positive:   positive,
		ReviewText: reviewText,
	}, totalRead, nil
}

func DeserializeRawReviewsBatchFromStrings(reviews []string, appIdIndex int, reviewScoreIndex int, reviewTextIndex int) ([]*RawReview, error) {
	rawReviews := make([]*RawReview, 0)

	for _, line := range reviews {
		reader := csv.NewReader(strings.NewReader(line))
		records, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		rawReview, err := NewRawReviewFromStrings(records[appIdIndex], records[reviewScoreIndex], records[reviewTextIndex])
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
