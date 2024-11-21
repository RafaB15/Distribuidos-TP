package persistence

import (
	n "distribuidos-tp/internal/system_protocol/node"
	rv "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	p "distribuidos-tp/persistence"
	"fmt"
	"github.com/op/go-logging"
	"sync"
)

const (
	AccumulatedRawReviewsFileName = "accumulated_raw_reviews"
	GamesToSendFileName           = "games_to_send"
)

type Repository struct {
	accumulatedRawReviewsPersister *p.Persister[*n.IntMap[*n.IntMap[[]*rv.RawReview]]]
	gamesToSendPersister           *p.Persister[*n.IntMap[*n.IntMap[bool]]]
	messageTrackerPersister        *p.Persister[*n.MessageTracker]
	logger                         *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	rawReviewsMap := n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
	accumulatedRawReviewsMap := n.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
	accumulatedRawReviewsPersister := p.NewPersister(AccumulatedRawReviewsFileName, accumulatedRawReviewsMap.Serialize, accumulatedRawReviewsMap.Deserialize, wg, logger)

	gamesToSendMap := n.NewIntMap(u.SerializeBool, u.DeserializeBool)
	accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
	gamesToSendPersister := p.NewPersister(GamesToSendFileName, accumulatedGamesToSendMap.Serialize, accumulatedGamesToSendMap.Deserialize, wg, logger)

	messageTrackerPersister := p.NewPersister("message_tracker", n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		accumulatedRawReviewsPersister: accumulatedRawReviewsPersister,
		gamesToSendPersister:           gamesToSendPersister,
		messageTrackerPersister:        messageTrackerPersister,
		logger:                         logger,
	}
}

func (r *Repository) SaveAccumulatedRawReviews(accumulatedRawReviewsMap *n.IntMap[*n.IntMap[[]*rv.RawReview]]) error {
	return r.accumulatedRawReviewsPersister.Save(accumulatedRawReviewsMap)
}

func (r *Repository) LoadAccumulatedRawReviews() *n.IntMap[*n.IntMap[[]*rv.RawReview]] {
	accumulatedRawReviews, err := r.accumulatedRawReviewsPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load accumulated raw reviews from file: %v. Returning new one", err)
		rawReviewsMap := n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
		accumulatedRawReviewsMap := n.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
		return accumulatedRawReviewsMap
	}
	return accumulatedRawReviews
}

func (r *Repository) SaveGamesToSend(gamesToSendMap *n.IntMap[*n.IntMap[bool]]) error {
	return r.gamesToSendPersister.Save(gamesToSendMap)
}

func (r *Repository) LoadGamesToSend() *n.IntMap[*n.IntMap[bool]] {
	gamesToSend, err := r.gamesToSendPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load games to send from file: %v. Returning new one", err)
		gamesToSendMap := n.NewIntMap(u.SerializeBool, u.DeserializeBool)
		accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
		return accumulatedGamesToSendMap
	}
	return gamesToSend
}

func (r *Repository) SaveMessageTracker(messageTracker *n.MessageTracker) error {
	return r.messageTrackerPersister.Save(messageTracker)
}

func (r *Repository) LoadMessageTracker(expectedEOFs int) *n.MessageTracker {
	messageTracker, err := r.messageTrackerPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load message tracker from file: %v. Returning new one", err)
		return n.NewMessageTracker(expectedEOFs)
	}
	return messageTracker
}

func (r *Repository) InitializeRawReviewMap() *n.IntMap[[]*rv.RawReview] {
	return n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
}

func (r *Repository) InitializeGamesToSendMap() *n.IntMap[bool] {
	return n.NewIntMap(u.SerializeBool, u.DeserializeBool)
}

func (r *Repository) SaveAll(accumulatedRawReviewsMap *n.IntMap[*n.IntMap[[]*rv.RawReview]], gamesToSendMap *n.IntMap[*n.IntMap[bool]], messageTracker *n.MessageTracker) error {
	err := r.SaveAccumulatedRawReviews(accumulatedRawReviewsMap)
	if err != nil {
		return fmt.Errorf("failed to save accumulated raw reviews: %v", err)
	}

	err = r.SaveGamesToSend(gamesToSendMap)
	if err != nil {
		return fmt.Errorf("failed to save games to send: %v", err)
	}

	err = r.SaveMessageTracker(messageTracker)
	if err != nil {
		return fmt.Errorf("failed to save message tracker: %v", err)
	}

	return nil
}
