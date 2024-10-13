package joiner

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	g "distribuidos-tp/internal/system_protocol/games"
	"encoding/binary"
	"strconv"
)

type JoinedActionNegativeGameReview struct {
	AppId           uint32
	GameName        string
	NegativeReviews int
}

func NewJoinedActionNegativeGameReview(appId uint32) *JoinedActionNegativeGameReview {
	return &JoinedActionNegativeGameReview{
		AppId:           appId,
		GameName:        "",
		NegativeReviews: 0,
	}
}

func (m *JoinedActionNegativeGameReview) UpdateWithReview(review *ra.GameReviewsMetrics) {
	m.NegativeReviews += review.NegativeReviews
}

func (m *JoinedActionNegativeGameReview) UpdateWithGame(game *g.GameName) {
	m.GameName = game.Name
}

func SerializeJoinedActionNegativeGameReview(metrics *JoinedActionNegativeGameReview) ([]byte, error) {
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

func DeserializeJoinedActionNegativeGameReview(data []byte) (*JoinedActionNegativeGameReview, error) {

	appId := binary.BigEndian.Uint32(data[0:4])

	gameNameLen := binary.BigEndian.Uint16(data[4:6])
	gameName := string(data[6 : 6+gameNameLen])

	NegativeReviewsStart := 6 + gameNameLen
	NegativeReviews := int(binary.BigEndian.Uint32(data[NegativeReviewsStart : NegativeReviewsStart+4]))

	return &JoinedActionNegativeGameReview{
		AppId:           appId,
		GameName:        gameName,
		NegativeReviews: NegativeReviews,
	}, nil
}

func GetStrRepresentationNegativeGameReview(joinedActionNegativeGameReview *JoinedActionNegativeGameReview) string {
	return "AppID: " + strconv.Itoa(int(joinedActionNegativeGameReview.AppId)) + ", GameName: " + joinedActionNegativeGameReview.GameName + ", NegativeReviews: " + strconv.Itoa(int(joinedActionNegativeGameReview.NegativeReviews)) + "\n"
}
