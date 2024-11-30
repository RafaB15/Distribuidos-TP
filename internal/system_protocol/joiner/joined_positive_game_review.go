package joiner

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"encoding/binary"
	"fmt"
	"strconv"
)

type JoinedPositiveGameReview struct {
	AppId           uint32
	GameName        string
	PositiveReviews int
}

func NewJoinedPositiveGameReview(appId uint32) *JoinedPositiveGameReview {
	return &JoinedPositiveGameReview{
		AppId:           appId,
		GameName:        "",
		PositiveReviews: 0,
	}
}

func (m *JoinedPositiveGameReview) UpdateWithReview(review *ra.GameReviewsMetrics) {
	m.PositiveReviews += review.PositiveReviews
}

func (m *JoinedPositiveGameReview) UpdateWithGame(name string) {
	m.GameName = name
}

func SerializeJoinedPositiveGameReview(metrics *JoinedPositiveGameReview) ([]byte, error) {
	totalLen := 4 + 2 + len(metrics.GameName) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], metrics.AppId)

	gameNameLen := uint16(len(metrics.GameName))
	binary.BigEndian.PutUint16(buf[4:6], gameNameLen)
	copy(buf[6:6+gameNameLen], metrics.GameName)

	positiveReviewsStart := 6 + gameNameLen
	binary.BigEndian.PutUint32(buf[positiveReviewsStart:positiveReviewsStart+4], uint32(metrics.PositiveReviews))

	return buf, nil
}

func DeserializeJoinedPositiveGameReview(data []byte) (*JoinedPositiveGameReview, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("data too short to deserialize JoinedPositiveGameReview")
	}
	appId := binary.BigEndian.Uint32(data[0:4])

	gameNameLen := binary.BigEndian.Uint16(data[4:6])
	gameName := string(data[6 : 6+gameNameLen])

	positiveReviewsStart := 6 + gameNameLen
	positiveReviews := int(binary.BigEndian.Uint32(data[positiveReviewsStart : positiveReviewsStart+4]))

	return &JoinedPositiveGameReview{
		AppId:           appId,
		GameName:        gameName,
		PositiveReviews: positiveReviews,
	}, nil
}

func SerializeJoinedPositiveGameReviewsBatch(joinedActionGameReviews []*JoinedPositiveGameReview) []byte {
	count := len(joinedActionGameReviews)
	headerSize := 2
	body := make([]byte, headerSize) // 2 bytes para el count

	binary.BigEndian.PutUint16(body[:headerSize], uint16(count))
	offset := headerSize

	for _, joinedActionGameReview := range joinedActionGameReviews {
		serializedJoinedPositiveGameReview, err := SerializeJoinedPositiveGameReview(joinedActionGameReview)
		if err != nil {
			fmt.Errorf("failed to serialize joined positive game review: %v", err)
			return nil
		}
		body = append(body, serializedJoinedPositiveGameReview...)
		offset += len(serializedJoinedPositiveGameReview)
	}

	return body
}

func DeserializeJoinedPositiveGameReviewsBatch(data []byte) ([]*JoinedPositiveGameReview, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short for batch")
	}
	count := binary.BigEndian.Uint16(data[:2])
	offset := 2
	joinedActionGameReviews := make([]*JoinedPositiveGameReview, 0)

	for i := 0; i < int(count); i++ {
		joinedActionGameReview, err := DeserializeJoinedPositiveGameReview(data[offset:])
		if err != nil {
			return nil, err
		}
		joinedActionGameReviews = append(joinedActionGameReviews, joinedActionGameReview)
		offset += 4 + 2 + len(joinedActionGameReview.GameName) + 4
	}

	return joinedActionGameReviews, nil
}

func GetStrRepresentation(joinedActionGameReview *JoinedPositiveGameReview) string {
	return "AppID: " + strconv.Itoa(int(joinedActionGameReview.AppId)) + ", GameName: " + joinedActionGameReview.GameName + ", PositiveReviews: " + strconv.Itoa(joinedActionGameReview.PositiveReviews) + "\n"
}
