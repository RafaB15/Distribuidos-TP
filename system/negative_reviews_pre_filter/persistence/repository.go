package persistence

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	p "distribuidos-tp/persistence"
	"github.com/op/go-logging"
	"sync"
)

const (
	AccumulatedRawReviewsFileName = "accumulated_raw_reviews"
	GamesToSendFileName           = "games_to_send"
)

type Repository struct {
	accumulatedRawReviewsPersister *p.Persister[*p.IntMap[*p.IntMap[[]*r.RawReview]]]
	gamesToSendPersister           *p.Persister[*p.IntMap[*p.IntMap[bool]]]
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	rawReviewsMap := p.NewIntMap(r.SerializeRawReviewsBatch, r.DeserializeRawReviewsBatch)
	accumulatedRawReviewsMap := p.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
	accumulatedRawReviewsPersister := p.NewPersister(AccumulatedRawReviewsFileName, accumulatedRawReviewsMap.Serialize, accumulatedRawReviewsMap.Deserialize, wg, logger)

	gamesToSendMap := p.NewIntMap(u.SerializeBool, u.DeserializeBool)
	accumulatedGamesToSendMap := p.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
	gamesToSendPersister := p.NewPersister(GamesToSendFileName, accumulatedGamesToSendMap.Serialize, accumulatedGamesToSendMap.Deserialize, wg, logger)

	return &Repository{
		accumulatedRawReviewsPersister: accumulatedRawReviewsPersister,
		gamesToSendPersister:           gamesToSendPersister,
	}
}
