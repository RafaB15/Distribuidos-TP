package joiner

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	g "distribuidos-tp/internal/system_protocol/games"
	"encoding/binary"
	"strconv"
)

type JoinedNegativeGameReview struct {
	AppId           uint32
	GameName        string
	NegativeReviews int
}

func NewJoinedActionNegativeGameReview(appId uint32) *JoinedNegativeGameReview {
	return &JoinedNegativeGameReview{
		AppId:           appId,
		GameName:        "",
		NegativeReviews: 0,
	}
}

func (m *JoinedNegativeGameReview) UpdateWithReview(review *ra.GameReviewsMetrics) {
	m.NegativeReviews += review.NegativeReviews
}

func (m *JoinedNegativeGameReview) UpdateWithGame(game *g.GameName) {
	m.GameName = game.Name
}

func SerializeJoinedActionNegativeGameReview(metrics *JoinedNegativeGameReview) ([]byte, error) {
	totalLen := 4 + 2 + len(metrics.GameName) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], metrics.AppId)

	gameNameLen := uint16(len(metrics.GameName))
	binary.BigEndian.PutUint16(buf[4:6], gameNameLen)
	copy(buf[6:6+gameNameLen], []byte(metrics.GameName))

	NegativeReviewsStart := 6 + gameNameLen
	binary.BigEndian.PutUint32(buf[NegativeReviewsStart:NegativeReviewsStart+4], uint32(metrics.NegativeReviews))

	return buf, nil
}

func DeserializeJoinedActionNegativeGameReview(data []byte) (*JoinedNegativeGameReview, error) {

	appId := binary.BigEndian.Uint32(data[0:4])

	gameNameLen := binary.BigEndian.Uint16(data[4:6])
	gameName := string(data[6 : 6+gameNameLen])

	NegativeReviewsStart := 6 + gameNameLen
	NegativeReviews := int(binary.BigEndian.Uint32(data[NegativeReviewsStart : NegativeReviewsStart+4]))

	return &JoinedNegativeGameReview{
		AppId:           appId,
		GameName:        gameName,
		NegativeReviews: NegativeReviews,
	}, nil
}

func SerializeJoinedActionNegativeGameReviewsBatch(joinedActionGameReviews []*JoinedNegativeGameReview) ([]byte, error) {
	count := len(joinedActionGameReviews)
	headerSize := 2
	body := make([]byte, headerSize)

	binary.BigEndian.PutUint16(body[:headerSize], uint16(count))
	offset := headerSize
	for _, joinedGameReview := range joinedActionGameReviews {
		serializedJoinedGameReview, err := SerializeJoinedActionNegativeGameReview(joinedGameReview)
		if err != nil {
			return nil, err
		}
		body = append(body, serializedJoinedGameReview...)
		offset += len(serializedJoinedGameReview)
	}
	return body, nil
}

func DeserializeJoinedActionNegativeGameReviewsBatch(data []byte) ([]*JoinedNegativeGameReview, error) {
	count := int(binary.BigEndian.Uint16(data[:2]))
	offset := 2
	joinedGameReviews := make([]*JoinedNegativeGameReview, 0)
	for i := 0; i < count; i++ {
		joinedGameReview, err := DeserializeJoinedActionNegativeGameReview(data[offset:])
		if err != nil {
			return nil, err
		}
		joinedGameReviews = append(joinedGameReviews, joinedGameReview)
		offset += 4 + 2 + len(joinedGameReview.GameName) + 4
	}
	return joinedGameReviews, nil
}

func GetStrRepresentationNegativeGameReview(joinedActionNegativeGameReview *JoinedNegativeGameReview) string {
	return "AppID: " + strconv.Itoa(int(joinedActionNegativeGameReview.AppId)) + ", GameName: " + joinedActionNegativeGameReview.GameName + ", NegativeReviews: " + strconv.Itoa(int(joinedActionNegativeGameReview.NegativeReviews)) + "\n"
}

func GetStrRepresentationNegativeGameReviewOnlyName(joinedActionNegativeGameReview *JoinedNegativeGameReview) string {
	return "AppID: " + strconv.Itoa(int(joinedActionNegativeGameReview.AppId)) + ", GameName: " + joinedActionNegativeGameReview.GameName + "\n"
}
