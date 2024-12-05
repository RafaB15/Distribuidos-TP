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
	negativeReviewsPersister *p.Persister[*n.IntMap[[]*ra.NamedGameReviewsMetrics]]
	messageTrackerPersister  *p.Persister[*n.MessageTracker]
	logger                   *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	negativeReviewsMap := n.NewIntMap(ra.SerializeNamedGameReviewsMetricsBatch, ra.DeserializeNamedGameReviewsMetricsBatch)
	negativeReviewsPersister := p.NewPersister(AccumulatedReviewsFileName, negativeReviewsMap.Serialize, negativeReviewsMap.Deserialize, wg, logger)

	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		negativeReviewsPersister: negativeReviewsPersister,
		messageTrackerPersister:  messageTrackerPersister,
		logger:                   logger,
	}
}

func (r *Repository) SaveNegativeReviews(accumulatedReviewsMap *n.IntMap[[]*ra.NamedGameReviewsMetrics], syncNumber uint64) error {
	return r.negativeReviewsPersister.Save(accumulatedReviewsMap, syncNumber)
}

func (r *Repository) LoadNegativeReviews(backup bool) (*n.IntMap[[]*ra.NamedGameReviewsMetrics], uint64) {
	var negativeReviews *n.IntMap[[]*ra.NamedGameReviewsMetrics]
	var syncNumber uint64
	var err error
	if backup {
		negativeReviews, syncNumber, err = r.negativeReviewsPersister.LoadBackupFile()
	} else {
		negativeReviews, syncNumber, err = r.negativeReviewsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load negative reviews from file: %v. Returning new one", err)
		negativeReviews = n.NewIntMap(ra.SerializeNamedGameReviewsMetricsBatch, ra.DeserializeNamedGameReviewsMetricsBatch)
		return negativeReviews, 0
	}
	return negativeReviews, syncNumber
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

func (r *Repository) SaveAll(negativeReviewsMap *n.IntMap[[]*ra.NamedGameReviewsMetrics], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveNegativeReviews(negativeReviewsMap, syncNumber)
	if err != nil {
		return err
	}

	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[[]*ra.NamedGameReviewsMetrics], *n.MessageTracker, uint64, error) {
	negativeReviews, negativeReviewsSyncNumber := r.LoadNegativeReviews(false)
	r.logger.Infof("Loaded negative reviews with sync number %d", negativeReviewsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := min(negativeReviewsSyncNumber, messageTrackerSyncNumber)

	if negativeReviewsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing accumulated reviews from %d, because min is %d", negativeReviewsSyncNumber, minSyncNumber)
		err := r.negativeReviewsPersister.Rollback()

		if err != nil {
			r.logger.Errorf("Failed to rollback accumulated reviews: %v", err)
			return nil, nil, 0, err
		}

		negativeReviews, negativeReviewsSyncNumber = r.LoadNegativeReviews(true)
		r.logger.Infof("Loaded negative reviews from backup with sync number %d", negativeReviewsSyncNumber)
	}

	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing message tracker from %d, because min is %d", messageTrackerSyncNumber, minSyncNumber)
		err := r.messageTrackerPersister.Rollback()

		if err != nil {
			r.logger.Errorf("Failed to rollback message tracker: %v", err)
			return nil, nil, 0, err
		}

		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
		r.logger.Infof("Loaded message tracker from backup with sync number %d", messageTrackerSyncNumber)
	}

	if messageTrackerSyncNumber != negativeReviewsSyncNumber {
		r.logger.Errorf("Message tracker sync number %d is different from negative reviews sync number %d", messageTrackerSyncNumber, negativeReviewsSyncNumber)
		return nil, nil, 0, fmt.Errorf("message tracker sync number %d is different from negative reviews sync number %d", messageTrackerSyncNumber, negativeReviewsSyncNumber)
	}

	return negativeReviews, messageTracker, messageTrackerSyncNumber, nil
}
