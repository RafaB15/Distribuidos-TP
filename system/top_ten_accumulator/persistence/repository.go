package persistence

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"sync"

	"github.com/op/go-logging"
)

const (
	MessageTrackerFileName = "message_tracker"
	TopTenFileName         = "os_final_metrics"
)

type Repository struct {
	messageTrackerPersister *p.Persister[*n.MessageTracker]
	yearAvgPtfPersister     *p.Persister[*n.IntMap[[]*df.GameYearAndAvgPtf]]
	logger                  *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)
	yearAvgPtfMap := n.NewIntMap(df.SerializeTopTenAvgPlaytimeForever, df.DeserializeTopTenAvgPlaytimeForever)
	yearAvgPtfPersister := p.NewPersister(TopTenFileName, yearAvgPtfMap.Serialize, yearAvgPtfMap.Deserialize, wg, logger)

	return &Repository{
		messageTrackerPersister: messageTrackerPersister,
		yearAvgPtfPersister:     yearAvgPtfPersister,
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
func (r *Repository) SaveTopTenMetrics(topTenMetrics *n.IntMap[[]*df.GameYearAndAvgPtf], syncNumber uint64) error {
	return r.yearAvgPtfPersister.Save(topTenMetrics, syncNumber)
}

func (r *Repository) LoadTopTenMetrics(backup bool) (*n.IntMap[[]*df.GameYearAndAvgPtf], uint64) {
	var topTenMetrics *n.IntMap[[]*df.GameYearAndAvgPtf]
	var syncNumber uint64
	var err error
	if backup {
		topTenMetrics, syncNumber, err = r.yearAvgPtfPersister.LoadBackupFile()
	} else {
		topTenMetrics, syncNumber, err = r.yearAvgPtfPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load top ten metrics from file: %v. Returning new one", err)
		topTenMetrics = n.NewIntMap(df.SerializeTopTenAvgPlaytimeForever, df.DeserializeTopTenAvgPlaytimeForever)
	}
	return topTenMetrics, syncNumber
}

func (r *Repository) SaveAll(topTenMetrics *n.IntMap[[]*df.GameYearAndAvgPtf], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveTopTenMetrics(topTenMetrics, syncNumber)
	if err != nil {
		return err
	}
	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[[]*df.GameYearAndAvgPtf], *n.MessageTracker, uint64, error) {
	topTenMetrics, topTenMetricsSyncNumber := r.LoadTopTenMetrics(false)
	r.logger.Infof("Loaded top ten metrics with sync number: %d", topTenMetricsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number: %d", messageTrackerSyncNumber)

	minSyncNumber := messageTrackerSyncNumber
	if topTenMetricsSyncNumber < minSyncNumber {
		minSyncNumber = topTenMetricsSyncNumber
	}

	if topTenMetricsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing top ten metrics from %d, because min is %d", topTenMetricsSyncNumber, minSyncNumber)
		err := r.yearAvgPtfPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback top ten metrics: %v", err)
			return nil, nil, 0, err
		}
		topTenMetrics, topTenMetricsSyncNumber = r.LoadTopTenMetrics(true)
		r.logger.Infof("Loaded backup top ten metrics with sync number: %d", topTenMetricsSyncNumber)
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

	if messageTrackerSyncNumber != topTenMetricsSyncNumber {
		return nil, nil, 0, fmt.Errorf("message tracker and top ten metrics sync numbers do not match: %d vs %d", messageTrackerSyncNumber, topTenMetricsSyncNumber)
	}

	return topTenMetrics, messageTracker, messageTrackerSyncNumber, nil
}
