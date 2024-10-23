package joiner

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	g "distribuidos-tp/internal/system_protocol/games"
	"encoding/binary"
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

func (m *JoinedPositiveGameReview) UpdateWithGame(game *g.GameName) {
	m.GameName = game.Name
}

func SerializeJoinedPositiveGameReview(metrics *JoinedPositiveGameReview) ([]byte, error) {
	totalLen := 4 + 2 + len(metrics.GameName) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], metrics.AppId)

	gameNameLen := uint16(len(metrics.GameName))
	binary.BigEndian.PutUint16(buf[4:6], gameNameLen)
	copy(buf[6:6+gameNameLen], []byte(metrics.GameName))

	positiveReviewsStart := 6 + gameNameLen
	binary.BigEndian.PutUint32(buf[positiveReviewsStart:positiveReviewsStart+4], uint32(metrics.PositiveReviews))

	return buf, nil
}

func DeserializeJoinedPositiveGameReview(data []byte) (*JoinedPositiveGameReview, error) {

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

func GetStrRepresentation(joinedActionGameReview *JoinedPositiveGameReview) string {
	return "AppID: " + strconv.Itoa(int(joinedActionGameReview.AppId)) + ", GameName: " + joinedActionGameReview.GameName + ", PositiveReviews: " + strconv.Itoa(int(joinedActionGameReview.PositiveReviews)) + "\n"
}
