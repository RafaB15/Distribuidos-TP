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
	eofControllerPersister         *p.Persister[*n.EndOfFileController]
	logger                         *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	rawReviewsMap := n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
	accumulatedRawReviewsMap := n.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
	accumulatedRawReviewsPersister := p.NewPersister(AccumulatedRawReviewsFileName, accumulatedRawReviewsMap.Serialize, accumulatedRawReviewsMap.Deserialize, wg, logger)

	gamesToSendMap := n.NewIntMap(u.SerializeBool, u.DeserializeBool)
	accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
	gamesToSendPersister := p.NewPersister(GamesToSendFileName, accumulatedGamesToSendMap.Serialize, accumulatedGamesToSendMap.Deserialize, wg, logger)

	eofControllerPersister := p.NewPersister("eof_controller", n.SerializeEndOfFileController, n.DeserializeEndOfFileController, wg, logger)

	return &Repository{
		accumulatedRawReviewsPersister: accumulatedRawReviewsPersister,
		gamesToSendPersister:           gamesToSendPersister,
		eofControllerPersister:         eofControllerPersister,
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

func (r *Repository) SaveEOFController(eofController *n.EndOfFileController) error {
	return r.eofControllerPersister.Save(eofController)
}

func (r *Repository) LoadEOFController(expectedEOFs int) *n.EndOfFileController {
	eofController, err := r.eofControllerPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load EOF controller from file: %v. Returning new one", err)
		return n.NewEndOfFileController(expectedEOFs)
	}
	return eofController
}

func (r *Repository) InitializeRawReviewMap() *n.IntMap[[]*rv.RawReview] {
	return n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
}

func (r *Repository) InitializeGamesToSendMap() *n.IntMap[bool] {
	return n.NewIntMap(u.SerializeBool, u.DeserializeBool)
}

func (r *Repository) SaveAll(accumulatedRawReviewsMap *n.IntMap[*n.IntMap[[]*rv.RawReview]], gamesToSendMap *n.IntMap[*n.IntMap[bool]], eofController *n.EndOfFileController) error {
	err := r.SaveAccumulatedRawReviews(accumulatedRawReviewsMap)
	if err != nil {
		return fmt.Errorf("failed to save accumulated raw reviews: %v", err)
	}

	err = r.SaveGamesToSend(gamesToSendMap)
	if err != nil {
		return fmt.Errorf("failed to save games to send: %v", err)
	}

	err = r.SaveEOFController(eofController)
	if err != nil {
		return fmt.Errorf("failed to save EOF controller: %v", err)
	}

	return nil
}
