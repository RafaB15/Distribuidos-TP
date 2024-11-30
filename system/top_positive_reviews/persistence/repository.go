package persistence

import (
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"

	"sync"

	"github.com/op/go-logging"
)

const (
	MessageTrackerFileName     = "message_tracker"
	TopPositiveReviewsFileName = "top_positive_reviews_file"
)

type Repository struct {
	messageTrackerPersister     *p.Persister[*n.MessageTracker]
	topPositiveReviewsPersister *p.Persister[*n.IntMap[[]*j.JoinedPositiveGameReview]]
	logger                      *logging.Logger
}

func DeserializeJoinedPositiveGameReviewsBatchWrapper(data []byte) []*j.JoinedPositiveGameReview {
	positiveReviews, err := j.DeserializeJoinedPositiveGameReviewsBatch(data)
	if err != nil {
		fmt.Errorf("failed to deserialize joined positive game reviews batch: %v", err)
		return nil
	}
	return positiveReviews
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	topPositiveReviewsMap := n.NewIntMap(j.SerializeJoinedPositiveGameReviewsBatch, j.DeserializeJoinedPositiveGameReviewsBatch)
	topPositiveReviewsPersister := p.NewPersister(TopPositiveReviewsFileName, topPositiveReviewsMap.Serialize, topPositiveReviewsMap.Deserialize, wg, logger)

	return &Repository{
		messageTrackerPersister:     messageTrackerPersister,
		topPositiveReviewsPersister: topPositiveReviewsPersister,
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

func (r *Repository) SaveTopPositiveReviews(topPositiveReviewsMap *n.IntMap[[]*j.JoinedPositiveGameReview], syncNumber uint64) error {
	return r.topPositiveReviewsPersister.Save(topPositiveReviewsMap, syncNumber)
}

func (r *Repository) LoadTopPositiveReviews(backup bool) (*n.IntMap[[]*j.JoinedPositiveGameReview], uint64) {
	var topPositiveReviews *n.IntMap[[]*j.JoinedPositiveGameReview]
	var syncNumber uint64
	var err error
	if backup {
		topPositiveReviews, syncNumber, err = r.topPositiveReviewsPersister.LoadBackupFile()
	} else {
		topPositiveReviews, syncNumber, err = r.topPositiveReviewsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load top positive reviews from file: %v. Returning new one", err)
		topPositiveReviews = n.NewIntMap(j.SerializeJoinedPositiveGameReviewsBatch, j.DeserializeJoinedPositiveGameReviewsBatch)
	}
	return topPositiveReviews, syncNumber
}

func (r *Repository) SaveAll(topPositiveReviewsMap *n.IntMap[[]*j.JoinedPositiveGameReview], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveTopPositiveReviews(topPositiveReviewsMap, syncNumber)
	if err != nil {
		return err
	}
	return r.SaveMessageTracker(messageTracker, syncNumber)
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[[]*j.JoinedPositiveGameReview], *n.MessageTracker, uint64, error) {
	topPositiveReviews, topPositiveReviewsSyncNumber := r.LoadTopPositiveReviews(false)
	r.logger.Infof("Loaded top positive reviews with sync number %d", topPositiveReviewsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := min(topPositiveReviewsSyncNumber, messageTrackerSyncNumber)
	if topPositiveReviewsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing top positive reviews from %d, because min is %d", topPositiveReviewsSyncNumber, minSyncNumber)
		err := r.topPositiveReviewsPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback top positive reviews persister: %v", err)
			return nil, nil, 0, err
		}
		topPositiveReviews, topPositiveReviewsSyncNumber = r.LoadTopPositiveReviews(true)
		r.logger.Infof("Loaded top positive reviews with sync number %d", topPositiveReviewsSyncNumber)
	}

	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing message tracker from %d, because min is %d", messageTrackerSyncNumber, minSyncNumber)
		err := r.messageTrackerPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback message tracker persister: %v", err)
			return nil, nil, 0, err
		}
		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
		r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)
	}

	if messageTrackerSyncNumber != topPositiveReviewsSyncNumber {
		r.logger.Errorf("Sync number mismatch after rollback: top positive reviews: %d, message tracker: %d", topPositiveReviewsSyncNumber, messageTrackerSyncNumber)
		return nil, nil, 0, fmt.Errorf("sync number mismatch")
	}
	return topPositiveReviews, messageTracker, minSyncNumber, nil
}
