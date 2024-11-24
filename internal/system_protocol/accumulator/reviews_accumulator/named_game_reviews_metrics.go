package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"errors"
	"strconv"
)

type NamedGameReviewsMetrics struct {
	AppID           uint32
	Name            string
	PositiveReviews int
	NegativeReviews int
}

func NewNamedGameReviewsMetrics(appId uint32, name string) *NamedGameReviewsMetrics {

	return &NamedGameReviewsMetrics{
		AppID:           appId,
		Name:            name,
		PositiveReviews: 0,
		NegativeReviews: 0,
	}
}

func (m *NamedGameReviewsMetrics) UpdateWithReview(review *r.ReducedReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func (m *NamedGameReviewsMetrics) UpdateWithRawReview(review *r.RawReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func SerializeNamedGameReviewsMetrics(metrics *NamedGameReviewsMetrics) []byte {
	nameLengthSize := 2
	nameLength := len(metrics.Name)

	buf := make([]byte, 4+nameLengthSize+nameLength+8)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:offset+4], metrics.AppID)
	offset += 4

	binary.BigEndian.PutUint16(buf[offset:offset+nameLengthSize], uint16(nameLength))
	offset += nameLengthSize

	copy(buf[offset:offset+nameLength], metrics.Name)
	offset += nameLength

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(metrics.PositiveReviews))
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(metrics.NegativeReviews))

	return buf
}

func DeserializeNamedGameReviewsMetrics(data []byte) (*NamedGameReviewsMetrics, int, error) {
	if len(data) < 14 {
		return nil, 0, errors.New("data too short to deserialize into NamedGameReviewsMetrics")
	}

	offset := 0

	appId := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	nameLength := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if len(data[offset:]) < int(nameLength) {
		return nil, 0, errors.New("invalid data length")
	}

	name := string(data[offset : offset+int(nameLength)])
	offset += int(nameLength)

	positiveReviews := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	negativeReviews := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	metrics := &NamedGameReviewsMetrics{
		AppID:           appId,
		Name:            name,
		PositiveReviews: positiveReviews,
		NegativeReviews: negativeReviews,
	}

	return metrics, offset, nil
}

func SerializeNamedGameReviewsMetricsBatch(metrics []*NamedGameReviewsMetrics) []byte {
	count := len(metrics)
	headerSize := 2 // 2 bytes for count

	body := make([]byte, headerSize)

	binary.BigEndian.PutUint16(body[:headerSize], uint16(count))

	for _, metric := range metrics {
		serializedMetric := SerializeNamedGameReviewsMetrics(metric)
		body = append(body, serializedMetric...)
	}

	return body
}

func DeserializeNamedGameReviewsMetricsBatch(data []byte) ([]*NamedGameReviewsMetrics, error) {
	amountSize := 2

	if len(data) < amountSize {
		return nil, errors.New("data too short to contain count")
	}

	count := int(binary.BigEndian.Uint16(data[:amountSize]))
	offset := amountSize

	var metrics []*NamedGameReviewsMetrics
	for i := 0; i < count; i++ {
		metric, bytesRead, err := DeserializeNamedGameReviewsMetrics(data[offset:])
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
		offset += bytesRead
	}

	return metrics, nil
}

func GetStrRepresentationGameReviewsMetricsOnlyName(namedGameReviewsMetrics *NamedGameReviewsMetrics) string {
	return "AppID: " + strconv.Itoa(int(namedGameReviewsMetrics.AppID)) + ", GameName: " + namedGameReviewsMetrics.Name + "\n"
}

func GetStrRepresentationGameReviewsMetrics(namedGameReviewsMetrics *NamedGameReviewsMetrics) string {
	return "AppID: " + strconv.Itoa(int(namedGameReviewsMetrics.AppID)) + ", GameName: " + namedGameReviewsMetrics.Name + ", NegativeReviews: " + strconv.Itoa(namedGameReviewsMetrics.NegativeReviews) + "\n"
}
