package persistence

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"sync"

	"github.com/op/go-logging"
)

const (
	MessageTrackerFileName        = "message_tracker"
	PercentileAccumulatorFileName = "percentile_acc"
)

type Repository struct {
	messageTrackerPersister *p.Persister[*n.MessageTracker]
	NamedReviewPersister    *p.Persister[*n.IntMap[[]*ra.NamedGameReviewsMetrics]]
	logger                  *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)
	namedReviewMap := n.NewIntMap(ra.SerializeNamedGameReviewsMetricsBatch, ra.DeserializeNamedGameReviewsMetricsBatch)
	namedReviewMapPersister := p.NewPersister(PercentileAccumulatorFileName, namedReviewMap.Serialize, namedReviewMap.Deserialize, wg, logger)

	return &Repository{
		messageTrackerPersister: messageTrackerPersister,
		NamedReviewPersister:    namedReviewMapPersister,
		logger:                  logger,
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

func (r *Repository) SaveNamedReviews(namedReviewsMap *n.IntMap[[]*ra.NamedGameReviewsMetrics], syncNumber uint64) error {
	return r.NamedReviewPersister.Save(namedReviewsMap, syncNumber)
}

func (r *Repository) LoadNamedReviews(backup bool) (*n.IntMap[[]*ra.NamedGameReviewsMetrics], uint64) {
	var namedReviews *n.IntMap[[]*ra.NamedGameReviewsMetrics]
	var syncNumber uint64
	var err error
	if backup {
		namedReviews, syncNumber, err = r.NamedReviewPersister.LoadBackupFile()
	} else {
		namedReviews, syncNumber, err = r.NamedReviewPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load named reviews from file: %v. Returning new one", err)
		namedReviews = n.NewIntMap(ra.SerializeNamedGameReviewsMetricsBatch, ra.DeserializeNamedGameReviewsMetricsBatch)
	}
	return namedReviews, syncNumber
}

func (r *Repository) SaveAll(namedReviewsMap *n.IntMap[[]*ra.NamedGameReviewsMetrics], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveNamedReviews(namedReviewsMap, syncNumber)
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
	namedReviewsMap, namedReviewsSyncNumber := r.LoadNamedReviews(false)
	r.logger.Infof("Loaded named reviews with sync number: %d", namedReviewsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number: %d", messageTrackerSyncNumber)

	minSyncNumber := messageTrackerSyncNumber
	if namedReviewsSyncNumber < minSyncNumber {
		minSyncNumber = namedReviewsSyncNumber
	}

	if namedReviewsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing named reviews from %d, because min is %d", namedReviewsSyncNumber, minSyncNumber)
		err := r.NamedReviewPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback named reviews: %v", err)
			return nil, nil, 0, err
		}
		namedReviewsMap, namedReviewsSyncNumber = r.LoadNamedReviews(true)
		r.logger.Infof("Loaded backup named reviews with sync number: %d", namedReviewsSyncNumber)
	}

	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing message tracker from %d, because min is %d", messageTrackerSyncNumber, minSyncNumber)
		err := r.messageTrackerPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback message tracker: %v", err)
		}
		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
		r.logger.Infof("Loaded backup message tracker with sync number: %d", messageTrackerSyncNumber)
	}

	if messageTrackerSyncNumber != namedReviewsSyncNumber {
		return nil, nil, 0, fmt.Errorf("message tracker and named reviews sync numbers do not match: %d vs %d", messageTrackerSyncNumber, namedReviewsSyncNumber)
	}

	return namedReviewsMap, messageTracker, messageTrackerSyncNumber, nil
}
