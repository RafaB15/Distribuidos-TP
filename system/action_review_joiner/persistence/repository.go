package persistence

import (
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	rv "distribuidos-tp/internal/system_protocol/reviews"
	p "distribuidos-tp/persistence"
	"fmt"
	"github.com/op/go-logging"
	"sync"
)

const (
	AccumulatedRawReviewsFileName = "accumulated_raw_reviews"
	GamesToSendFileName           = "games_to_send"
	MessageTrackerFileName        = "message_tracker"
)

type Repository struct {
	accumulatedRawReviewsPersister *p.Persister[*n.IntMap[*n.IntMap[[]*rv.RawReview]]]
	gamesToSendPersister           *p.Persister[*n.IntMap[*n.IntMap[*j.GameToSend]]]
	messageTrackerPersister        *p.Persister[*n.MessageTracker]
	logger                         *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	rawReviewsMap := n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
	accumulatedRawReviewsMap := n.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
	accumulatedRawReviewsPersister := p.NewPersister(AccumulatedRawReviewsFileName, accumulatedRawReviewsMap.Serialize, accumulatedRawReviewsMap.Deserialize, wg, logger)

	gamesToSendMap := n.NewIntMap(j.SerializeGameToSend, j.DeserializeGameToSend)
	accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
	gamesToSendPersister := p.NewPersister(GamesToSendFileName, accumulatedGamesToSendMap.Serialize, accumulatedGamesToSendMap.Deserialize, wg, logger)

	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		accumulatedRawReviewsPersister: accumulatedRawReviewsPersister,
		gamesToSendPersister:           gamesToSendPersister,
		messageTrackerPersister:        messageTrackerPersister,
		logger:                         logger,
	}
}

func (r *Repository) SaveAccumulatedRawReviews(accumulatedRawReviewsMap *n.IntMap[*n.IntMap[[]*rv.RawReview]], syncNumber uint64) error {
	return r.accumulatedRawReviewsPersister.Save(accumulatedRawReviewsMap, syncNumber)
}

func (r *Repository) LoadAccumulatedRawReviews(backup bool) (*n.IntMap[*n.IntMap[[]*rv.RawReview]], uint64) {
	var accumulatedRawReviews *n.IntMap[*n.IntMap[[]*rv.RawReview]]
	var syncNumber uint64
	var err error
	if backup {
		accumulatedRawReviews, syncNumber, err = r.accumulatedRawReviewsPersister.LoadBackupFile()
	} else {
		accumulatedRawReviews, syncNumber, err = r.accumulatedRawReviewsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load accumulated raw reviews from file: %v. Returning new one", err)
		rawReviewsMap := n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
		accumulatedRawReviewsMap := n.NewIntMap(rawReviewsMap.Serialize, rawReviewsMap.Deserialize)
		return accumulatedRawReviewsMap, 0
	}
	return accumulatedRawReviews, syncNumber
}

func (r *Repository) SaveGamesToSend(gamesToSendMap *n.IntMap[*n.IntMap[*j.GameToSend]], syncNumber uint64) error {
	return r.gamesToSendPersister.Save(gamesToSendMap, syncNumber)
}

func (r *Repository) LoadGamesToSend(backup bool) (*n.IntMap[*n.IntMap[*j.GameToSend]], uint64) {
	var gamesToSend *n.IntMap[*n.IntMap[*j.GameToSend]]
	var syncNumber uint64
	var err error

	if backup {
		gamesToSend, syncNumber, err = r.gamesToSendPersister.LoadBackupFile()
	} else {
		gamesToSend, syncNumber, err = r.gamesToSendPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load games to send from file: %v. Returning new one", err)
		gamesToSendMap := n.NewIntMap(j.SerializeGameToSend, j.DeserializeGameToSend)
		accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
		return accumulatedGamesToSendMap, 0
	}
	return gamesToSend, syncNumber
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

func (r *Repository) InitializeRawReviewMap() *n.IntMap[[]*rv.RawReview] {
	return n.NewIntMap(rv.SerializeRawReviewsBatch, rv.DeserializeRawReviewsBatch)
}

func (r *Repository) InitializeGamesToSendMap() *n.IntMap[*j.GameToSend] {
	return n.NewIntMap(j.SerializeGameToSend, j.DeserializeGameToSend)
}

func (r *Repository) SaveAll(accumulatedRawReviewsMap *n.IntMap[*n.IntMap[[]*rv.RawReview]], gamesToSendMap *n.IntMap[*n.IntMap[*j.GameToSend]], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveAccumulatedRawReviews(accumulatedRawReviewsMap, syncNumber)
	if err != nil {
		return fmt.Errorf("failed to save accumulated raw reviews: %v", err)
	}

	err = r.SaveGamesToSend(gamesToSendMap, syncNumber)
	if err != nil {
		return fmt.Errorf("failed to save games to send: %v", err)
	}

	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return fmt.Errorf("failed to save message tracker: %v", err)
	}

	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[*n.IntMap[[]*rv.RawReview]], *n.IntMap[*n.IntMap[*j.GameToSend]], *n.MessageTracker, uint64, error) {
	accumulatedRawReviewsMap, accumulatedRawReviewsMapSyncNumber := r.LoadAccumulatedRawReviews(false)
	r.logger.Infof("Loaded accumulated raw reviews with sync number %d", accumulatedRawReviewsMapSyncNumber)

	gamesToSendMap, gamesToSendMapSyncNumber := r.LoadGamesToSend(false)
	r.logger.Infof("Loaded games to send with sync number %d", gamesToSendMapSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := accumulatedRawReviewsMapSyncNumber
	if gamesToSendMapSyncNumber < minSyncNumber {
		minSyncNumber = gamesToSendMapSyncNumber
	}
	if messageTrackerSyncNumber < minSyncNumber {
		minSyncNumber = messageTrackerSyncNumber
	}

	if accumulatedRawReviewsMapSyncNumber > minSyncNumber {
		r.logger.Infof("Loading accumulated raw reviews from backup because min sync number is %d", minSyncNumber)
		err := r.accumulatedRawReviewsPersister.Rollback()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("failed to rollback accumulated raw reviews persister: %v", err)
		}
		accumulatedRawReviewsMap, accumulatedRawReviewsMapSyncNumber = r.LoadAccumulatedRawReviews(true)
	}
	if gamesToSendMapSyncNumber > minSyncNumber {
		r.logger.Infof("Loading games to send from backup because min sync number is %d", minSyncNumber)
		err := r.gamesToSendPersister.Rollback()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("failed to rollback games to send persister: %v", err)
		}
		gamesToSendMap, gamesToSendMapSyncNumber = r.LoadGamesToSend(true)
	}
	if messageTrackerSyncNumber > minSyncNumber {
		r.logger.Infof("Loading message tracker from backup because min sync number is %d", minSyncNumber)
		err := r.messageTrackerPersister.Rollback()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("failed to rollback message tracker persister: %v", err)
		}
		messageTracker, messageTrackerSyncNumber = r.LoadMessageTracker(expectedEOFs, true)
	}

	if accumulatedRawReviewsMapSyncNumber != gamesToSendMapSyncNumber || gamesToSendMapSyncNumber != messageTrackerSyncNumber {
		return nil, nil, nil, 0, fmt.Errorf("sync numbers don't match. Accumulated raw reviews: %d, games to send: %d, message tracker: %d", accumulatedRawReviewsMapSyncNumber, gamesToSendMapSyncNumber, messageTrackerSyncNumber)
	}

	return accumulatedRawReviewsMap, gamesToSendMap, messageTracker, accumulatedRawReviewsMapSyncNumber, nil
}
