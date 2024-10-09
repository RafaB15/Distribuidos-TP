package joiner

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	g "distribuidos-tp/internal/system_protocol/games"
	"encoding/binary"
)

type JoinedActionGameReview struct {
	AppId           uint32
	GameName        string
	PositiveReviews int
}

func NewJoinedActionGameReview(appId uint32) *JoinedActionGameReview {
	return &JoinedActionGameReview{
		AppId:           appId,
		GameName:        "",
		PositiveReviews: 0,
	}
}

func (m *JoinedActionGameReview) UpdateWithReview(review *ra.GameReviewsMetrics) {
	m.PositiveReviews += review.PositiveReviews
}

func (m *JoinedActionGameReview) UpdateWithGame(game *g.GameName) {
	m.GameName = game.Name
}

func SerializeJoinedActionGameReview(metrics *JoinedActionGameReview) ([]byte, error) {
	totalLen := 4 + 4 + len(metrics.GameName) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], metrics.AppId)

	gameNameLen := uint32(len(metrics.GameName))
	binary.BigEndian.PutUint32(buf[4:8], gameNameLen)
	copy(buf[8:8+gameNameLen], []byte(metrics.GameName))

	positiveReviewsStart := 8 + gameNameLen
	binary.BigEndian.PutUint32(buf[positiveReviewsStart:positiveReviewsStart+4], uint32(metrics.PositiveReviews))

	return buf, nil
}

func DeserializeJoinedActionGameReview(data []byte) (*JoinedActionGameReview, error) {

	appId := binary.BigEndian.Uint32(data[0:4])

	gameNameLen := binary.BigEndian.Uint32(data[4:8])
	gameName := string(data[8 : 8+gameNameLen])

	positiveReviewsStart := 8 + gameNameLen
	positiveReviews := int(binary.BigEndian.Uint32(data[positiveReviewsStart : positiveReviewsStart+4]))

	return &JoinedActionGameReview{
		AppId:           appId,
		GameName:        gameName,
		PositiveReviews: positiveReviews,
	}, nil
}
