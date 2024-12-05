package persistence

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"sync"

	"github.com/op/go-logging"
)

const (
	IndieReviewJoinerRepositoryFileName = "indie_review_joiner_repository"
	GamesToSendFileName                 = "games_to_send"
	MessageTrackerFileName              = "message_tracker"
)

type Repository struct {
	indieReviewJoinerRepositoryPersister *p.Persister[*n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]]]
	gamesToSendPersister                 *p.Persister[*n.IntMap[*n.IntMap[*j.GameToSend]]]
	messageTrackerPersister              *p.Persister[*n.MessageTracker]
	logger                               *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	joinedPositiveGameReviewsMap := n.NewIntMap(ra.SerializeGameReviewsMetrics, ra.DeserializeGameReviewsMetrics)
	acumulatedJoinedPositiveGameReviewsMap := n.NewIntMap(joinedPositiveGameReviewsMap.Serialize, joinedPositiveGameReviewsMap.Deserialize)
	indieReviewJoinerRepositoryPersister := p.NewPersister(IndieReviewJoinerRepositoryFileName, acumulatedJoinedPositiveGameReviewsMap.Serialize, acumulatedJoinedPositiveGameReviewsMap.Deserialize, wg, logger)

	gamesToSendMap := n.NewIntMap(j.SerializeGameToSend, j.DeserializeGameToSend)
	accumulatedGamesToSendMap := n.NewIntMap(gamesToSendMap.Serialize, gamesToSendMap.Deserialize)
	gamesToSendPersister := p.NewPersister(GamesToSendFileName, accumulatedGamesToSendMap.Serialize, accumulatedGamesToSendMap.Deserialize, wg, logger)

	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		indieReviewJoinerRepositoryPersister: indieReviewJoinerRepositoryPersister,
		gamesToSendPersister:                 gamesToSendPersister,
		messageTrackerPersister:              messageTrackerPersister,
		logger:                               logger,
	}
}

func (r *Repository) SaveIndieReviewJoiners(accumulatedJoinedReviewsMap *n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]], syncNumber uint64) error {
	return r.indieReviewJoinerRepositoryPersister.Save(accumulatedJoinedReviewsMap, syncNumber)
}

func (r *Repository) LoadIndieJoinedReviews(backup bool) (*n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]], uint64) {
	var accumulatedIndieJoinedReviews *n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]]
	var syncNumber uint64
	var err error

	if backup {
		accumulatedIndieJoinedReviews, syncNumber, err = r.indieReviewJoinerRepositoryPersister.LoadBackupFile()
	} else {
		accumulatedIndieJoinedReviews, syncNumber, err = r.indieReviewJoinerRepositoryPersister.Load()
	}

	if err != nil {
		r.logger.Errorf("Failed to load accumulated indie joined reviews from file: %v. Returning new one", err)
		joinedPositiveGameReviewsMap := n.NewIntMap(ra.SerializeGameReviewsMetrics, ra.DeserializeGameReviewsMetrics)
		accumulatedJoinedPositiveGameReviewsMap := n.NewIntMap(joinedPositiveGameReviewsMap.Serialize, joinedPositiveGameReviewsMap.Deserialize)
		return accumulatedJoinedPositiveGameReviewsMap, 0
	}
	return accumulatedIndieJoinedReviews, syncNumber
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

func (r *Repository) InitializeIndieJoinedReviewsMap() *n.IntMap[*ra.GameReviewsMetrics] {
	return n.NewIntMap(ra.SerializeGameReviewsMetrics, ra.DeserializeGameReviewsMetrics)
}

func (r *Repository) InitializeGamesToSendMap() *n.IntMap[*j.GameToSend] {
	return n.NewIntMap(j.SerializeGameToSend, j.DeserializeGameToSend)
}

func (r *Repository) SaveAll(accumulatedIndieJoinedReviewsMap *n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]], gamesToSendMap *n.IntMap[*n.IntMap[*j.GameToSend]], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveIndieReviewJoiners(accumulatedIndieJoinedReviewsMap, syncNumber)
	if err != nil {
		return err
	}

	err = r.SaveGamesToSend(gamesToSendMap, syncNumber)
	if err != nil {
		return err
	}

	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[*n.IntMap[*ra.GameReviewsMetrics]], *n.IntMap[*n.IntMap[*j.GameToSend]], *n.MessageTracker, uint64, error) {
	accumulatedIndieJoinedReviewsMap, accumulatedIndieJoinedReviewsSyncNumber := r.LoadIndieJoinedReviews(false)
	r.logger.Infof("Loaded accumulated Indie Joines Reviews with sync number %d", accumulatedIndieJoinedReviewsSyncNumber)

	gamesToSendMap, gamesToSendMapSyncNumber := r.LoadGamesToSend(false)
	r.logger.Infof("Loaded games to send with sync number %d", gamesToSendMapSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number %d", messageTrackerSyncNumber)

	minSyncNumber := accumulatedIndieJoinedReviewsSyncNumber
	if gamesToSendMapSyncNumber < minSyncNumber {
		minSyncNumber = gamesToSendMapSyncNumber
	}
	if messageTrackerSyncNumber < minSyncNumber {
		minSyncNumber = messageTrackerSyncNumber
	}

	if accumulatedIndieJoinedReviewsSyncNumber > minSyncNumber {
		r.logger.Infof("Loading accumulated Indie Joined Reviews from backup because min sync number is %d", minSyncNumber)
		err := r.indieReviewJoinerRepositoryPersister.Rollback()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("failed to rollback accumulated indie joined reviews map persister: %v", err)
		}
		accumulatedIndieJoinedReviewsMap, accumulatedIndieJoinedReviewsSyncNumber = r.LoadIndieJoinedReviews(true)
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

	if accumulatedIndieJoinedReviewsSyncNumber != gamesToSendMapSyncNumber || gamesToSendMapSyncNumber != messageTrackerSyncNumber {
		return nil, nil, nil, 0, fmt.Errorf("Sync numbers do not match: accumulatedIndieJoinedReviewsSyncNumber: %d, gamesToSendMapSyncNumber: %d, messageTrackerSyncNumber: %d", accumulatedIndieJoinedReviewsSyncNumber, gamesToSendMapSyncNumber, messageTrackerSyncNumber)
	}
	return accumulatedIndieJoinedReviewsMap, gamesToSendMap, messageTracker, accumulatedIndieJoinedReviewsSyncNumber, nil
}
