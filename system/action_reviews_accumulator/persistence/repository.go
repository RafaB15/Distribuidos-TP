package persistence

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"github.com/op/go-logging"
	"sync"
)

const (
	AccumulatedReviewsFileName = "accumulated_reviews"
	MessageTrackerFileName     = "message_tracker"
)

type Repository struct {
	accumulatedReviewsPersister *p.Persister[*n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]]]
	messageTrackerPersister     *p.Persister[*n.MessageTracker]
	logger                      *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	reviewsMap := n.NewIntMap(ra.SerializeNamedGameReviewsMetrics, ra.DeserializeNamedGameReviewsMetrics)
	accumulatedReviewsMap := n.NewIntMap(reviewsMap.Serialize, reviewsMap.Deserialize)
	accumulatedReviewsPersister := p.NewPersister(AccumulatedReviewsFileName, accumulatedReviewsMap.Serialize, accumulatedReviewsMap.Deserialize, wg, logger)

	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		accumulatedReviewsPersister: accumulatedReviewsPersister,
		messageTrackerPersister:     messageTrackerPersister,
		logger:                      logger,
	}
}

func (r *Repository) SaveAccumulatedReviews(accumulatedReviewsMap *n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]], syncNumber uint64) error {
	return r.accumulatedReviewsPersister.Save(accumulatedReviewsMap, syncNumber)
}

func (r *Repository) LoadAccumulatedReviews(backup bool) (*n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]], uint64) {
	var accumulatedReviews *n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]]
	var syncNumber uint64
	var err error
	if backup {
		accumulatedReviews, syncNumber, err = r.accumulatedReviewsPersister.LoadBackupFile()
	} else {
		accumulatedReviews, syncNumber, err = r.accumulatedReviewsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load accumulated reviews from file: %v. Returning new one", err)
		reviewsMap := n.NewIntMap(ra.SerializeNamedGameReviewsMetrics, ra.DeserializeNamedGameReviewsMetrics)
		accumulatedReviews = n.NewIntMap(reviewsMap.Serialize, reviewsMap.Deserialize)
		return accumulatedReviews, 0
	}
	return accumulatedReviews, syncNumber
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

func (r *Repository) InitializeAccumulatedReviewsMap() *n.IntMap[*ra.NamedGameReviewsMetrics] {
	return n.NewIntMap(ra.SerializeNamedGameReviewsMetrics, ra.DeserializeNamedGameReviewsMetrics)
}

func (r *Repository) SaveAll(accumulatedReviewsMap *n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]], messageTracker *n.MessageTracker, syncNumber uint64) error {
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

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[*n.IntMap[*ra.NamedGameReviewsMetrics]], *n.MessageTracker, uint64, error) {
	accumulatedReviewsMap, accumulatedReviewsMapSyncNumber := r.LoadAccumulatedReviews(false)
	r.logger.Infof("Loaded accumulated reviews map with sync number %d", accumulatedReviewsMapSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := min(accumulatedReviewsMapSyncNumber, messageTrackerSyncNumber)

	if accumulatedReviewsMapSyncNumber > minSyncNumber {
		r.logger.Infof("Loading accumulated reviews map from backup because min sync number is %d", minSyncNumber)
		err := r.accumulatedReviewsPersister.Rollback()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to rollback accumulated reviews map: %v", err)
		}
		accumulatedReviewsMap, accumulatedReviewsMapSyncNumber = r.LoadAccumulatedReviews(true)
	}

	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Loading message tracker from backup because min sync number is %d", minSyncNumber)
		err := r.messageTrackerPersister.Rollback()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to rollback message tracker persister: %v", err)
		}
		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
	}

	if accumulatedReviewsMapSyncNumber != messageTrackerSyncNumber {
		return nil, nil, 0, fmt.Errorf("sync numbers don't match. Accumulated reviews: %d, MessageTracker %d", accumulatedReviewsMapSyncNumber, messageTrackerSyncNumber)
	}

	return accumulatedReviewsMap, messageTracker, minSyncNumber, nil
}
