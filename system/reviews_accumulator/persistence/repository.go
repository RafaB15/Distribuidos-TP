package persistence

import (
	rg "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"sync"

	"github.com/op/go-logging"
)

const (
	MessageTrackerFileName     = "message_tracker"
	AccumulatedReviewsFileName = "accumulated_reviews"
)

type Repository struct {
	messageTrackerPersister     *p.Persister[*n.MessageTracker]
	accumulatedReviewsPersister *p.Persister[*n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]]]
	logger                      *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	reviewsMap := n.NewIntMap(rg.SerializeGameReviewsMetrics, rg.DeserializeGameReviewsMetrics)
	accumulatedReviewsMap := n.NewIntMap(reviewsMap.Serialize, reviewsMap.Deserialize)
	accumulatedReviewsPersister := p.NewPersister(AccumulatedReviewsFileName, accumulatedReviewsMap.Serialize, accumulatedReviewsMap.Deserialize, wg, logger)

	return &Repository{
		messageTrackerPersister:     messageTrackerPersister,
		accumulatedReviewsPersister: accumulatedReviewsPersister,
		logger:                      logger,
	}
}

func (r *Repository) SaveMessageTracker(messageTracker *n.MessageTracker, syncNumber uint64) error {
	return r.messageTrackerPersister.Save(messageTracker, syncNumber)
}
func (r *Repository) LoadMessageTracker(expectedEOFs int, backup bool) (*n.MessageTracker, uint64) {
	var messageTracker *n.MessageTracker
	var syncNumber uint64
	var err error
	if backup {
		messageTracker, syncNumber, err = r.messageTrackerPersister.LoadBackupFile()

	} else {
		messageTracker, syncNumber, err = r.messageTrackerPersister.Load()
	}

	if err != nil {
		r.logger.Errorf("Failed to load message tracker from file: %v. Returning new one", err)
		return n.NewMessageTracker(expectedEOFs), 0
	}
	return messageTracker, syncNumber
}

func (r *Repository) SaveAccumulatedReviews(accumulatedReviewsMap *n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]], syncNumber uint64) error {
	return r.accumulatedReviewsPersister.Save(accumulatedReviewsMap, syncNumber)
}

func (r *Repository) LoadAccumulatedReviews(backup bool) (*n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]], uint64) {
	var accumulatedReviewsMap *n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]]
	var syncNumber uint64
	var err error

	if backup {
		accumulatedReviewsMap, syncNumber, err = r.accumulatedReviewsPersister.LoadBackupFile()
	} else {
		accumulatedReviewsMap, syncNumber, err = r.accumulatedReviewsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load accumulated reviews from file: %v. Returning new one", err)
		reviewsMap := n.NewIntMap(rg.SerializeGameReviewsMetrics, rg.DeserializeGameReviewsMetrics)
		accumulatedReviewsMap = n.NewIntMap(reviewsMap.Serialize, reviewsMap.Deserialize)
	}
	return accumulatedReviewsMap, syncNumber
}

func (r *Repository) InitializeAccumulatedReviewsMap() *n.IntMap[*rg.GameReviewsMetrics] {
	return n.NewIntMap(rg.SerializeGameReviewsMetrics, rg.DeserializeGameReviewsMetrics)
}

func (r *Repository) SaveAll(accumulatedReviewsMap *n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveAccumulatedReviews(accumulatedReviewsMap, syncNumber)
	if err != nil {
		return err
	}
	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[*n.IntMap[*rg.GameReviewsMetrics]], *n.MessageTracker, uint64, error) {
	accumulatedReviewsMap, accumulatedReviewsSyncNumber := r.LoadAccumulatedReviews(false)
	r.logger.Infof("Loaded accumulated reviews with sync number %d", accumulatedReviewsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := min(messageTrackerSyncNumber, accumulatedReviewsSyncNumber)

	if accumulatedReviewsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing accumulated reviews from %d, because min is %d", accumulatedReviewsSyncNumber, minSyncNumber)
		err := r.accumulatedReviewsPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback accumulated reviews: %v", err)
			return nil, nil, 0, err
		}
		accumulatedReviewsMap, accumulatedReviewsSyncNumber = r.LoadAccumulatedReviews(true)
		r.logger.Infof("Loaded accumulated reviews backup file with sync number %d", accumulatedReviewsSyncNumber)
	}

	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing message tracker from %d, because min is %d", messageTrackerSyncNumber, minSyncNumber)
		err := r.messageTrackerPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback message tracker: %v", err)
			return nil, nil, 0, err
		}
		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
		r.logger.Infof("Loaded message tracker backup file with sync number %d", messageTrackerSyncNumber)
	}
	if messageTrackerSyncNumber != accumulatedReviewsSyncNumber {
		return nil, nil, 0, fmt.Errorf("messager tracker and accumulated reviews sync numbers do not match: %d != %d", messageTrackerSyncNumber, accumulatedReviewsSyncNumber)
	}

	return accumulatedReviewsMap, messageTracker, minSyncNumber, nil
}
