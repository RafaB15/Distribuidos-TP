package reviews

import (
	"encoding/binary"
	"errors"
)

type Review struct {
	ReviewId   uint32
	AppId      uint32
	Name       string
	Positive   bool
	ReviewText string
}

func NewReview(reviewId uint32, appId uint32, name string, positive bool, reviewText string) *Review {
	return &Review{
		ReviewId:   reviewId,
		AppId:      appId,
		Name:       name,
		Positive:   positive,
		ReviewText: reviewText,
	}
}

func (r *Review) Serialize() []byte {
	reviewIdSize := 4
	appIdSize := 4
	nameLengthSize := 2
	nameSize := len(r.Name)
	amountSize := 4
	boolSize := 1

	reviewSize := reviewIdSize + appIdSize + nameLengthSize + nameSize + boolSize + amountSize + len(r.ReviewText)

	offset := 0

	buf := make([]byte, reviewSize)

	binary.LittleEndian.PutUint32(buf[offset:reviewIdSize], r.ReviewId)
	offset += reviewIdSize

	binary.LittleEndian.PutUint32(buf[offset:offset+appIdSize], r.AppId)
	offset += appIdSize

	binary.LittleEndian.PutUint16(buf[offset:offset+nameLengthSize], uint16(nameSize))
	offset += nameLengthSize

	copy(buf[offset:offset+nameSize], r.Name)
	offset += nameSize

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

func DeserializeReview(buf []byte) (*Review, int, error) {
	reviewIdSize := 4
	appIdSize := 4
	nameLengthSize := 2
	amountSize := 4
	boolSize := 1

	if len(buf) < reviewIdSize+appIdSize+nameLengthSize+boolSize+amountSize {
		return nil, 0, errors.New("buffer too short")
	}

	offset := 0

	reviewId := binary.LittleEndian.Uint32(buf[offset : offset+reviewIdSize])
	offset += reviewIdSize

	appId := binary.LittleEndian.Uint32(buf[offset : offset+appIdSize])
	offset += appIdSize

	nameLength := binary.LittleEndian.Uint16(buf[offset : offset+nameLengthSize])
	offset += nameLengthSize

	if len(buf[offset:]) < int(nameLength) {
		return nil, 0, errors.New("buffer too short for name")
	}

	name := string(buf[offset : offset+int(nameLength)])
	offset += int(nameLength)

	positive := buf[offset] == 1
	offset += boolSize

	amount := binary.LittleEndian.Uint32(buf[offset : offset+amountSize])
	offset += amountSize

	if len(buf[offset:]) < int(amount) {
		return nil, 0, errors.New("buffer too short for review text")
	}

	reviewText := string(buf[offset : offset+int(amount)])
	offset += int(amount)

	return &Review{
		ReviewId:   reviewId,
		AppId:      appId,
		Name:       name,
		Positive:   positive,
		ReviewText: reviewText,
	}, offset, nil
}

func SerializeReviewsBatch(reviews []*Review) []byte {
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

func DeserializeReviewsBatch(buf []byte) ([]*Review, error) {
	if len(buf) < 2 {
		return nil, errors.New("buffer too short")
	}

	reviewCount := binary.LittleEndian.Uint16(buf[:2])
	buf = buf[2:]

	rawReviews := make([]*Review, 0)

	for i := 0; i < int(reviewCount); i++ {
		review, amountRead, err := DeserializeReview(buf)
		if err != nil {
			return nil, err
		}

		rawReviews = append(rawReviews, review)
		buf = buf[amountRead:]
	}

	return rawReviews, nil
}
