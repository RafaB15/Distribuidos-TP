package persistence

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"fmt"
	"github.com/op/go-logging"
	"sync"
)

const (
	MessageTrackerFileName = "message_tracker"
	OsMetricsFileName      = "os_metrics"
)

type Repository struct {
	messageTrackerPersister *p.Persister[*n.MessageTracker]
	osMetricsPersister      *p.Persister[*n.IntMap[*oa.GameOSMetrics]]
	logger                  *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	osMetricsMap := n.NewIntMap(oa.SerializeGameOSMetrics, oa.DeserializeGameOSMetrics)
	osMetricsPersister := p.NewPersister(OsMetricsFileName, osMetricsMap.Serialize, osMetricsMap.Deserialize, wg, logger)

	return &Repository{
		messageTrackerPersister: messageTrackerPersister,
		osMetricsPersister:      osMetricsPersister,
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

func (r *Repository) SaveOsMetrics(osMetricsMap *n.IntMap[*oa.GameOSMetrics], syncNumber uint64) error {
	return r.osMetricsPersister.Save(osMetricsMap, syncNumber)
}

func (r *Repository) LoadOsMetrics(backup bool) (*n.IntMap[*oa.GameOSMetrics], uint64) {
	var osMetrics *n.IntMap[*oa.GameOSMetrics]
	var syncNumber uint64
	var err error
	if backup {
		osMetrics, syncNumber, err = r.osMetricsPersister.LoadBackupFile()
	} else {
		osMetrics, syncNumber, err = r.osMetricsPersister.Load()
	}
	if err != nil {
		r.logger.Errorf("Failed to load os metrics from file: %v. Returning new one", err)
		osMetrics = n.NewIntMap(oa.SerializeGameOSMetrics, oa.DeserializeGameOSMetrics)
	}
	return osMetrics, syncNumber
}

func (r *Repository) SaveAll(osMetricsMap *n.IntMap[*oa.GameOSMetrics], messageTracker *n.MessageTracker, syncNumber uint64) error {
	err := r.SaveOsMetrics(osMetricsMap, syncNumber)
	if err != nil {
		return err
	}
	err = r.SaveMessageTracker(messageTracker, syncNumber)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) LoadAll(expectedEOFs int) (*n.IntMap[*oa.GameOSMetrics], *n.MessageTracker, uint64, error) {
	osMetricsMap, osMetricsSyncNumber := r.LoadOsMetrics(false)
	r.logger.Infof("Loaded os metrics with sync number: %d", osMetricsSyncNumber)

	messageTracker, messageTrackerSyncNumber := r.LoadMessageTracker(expectedEOFs, false)
	r.logger.Infof("Loaded message tracker with sync number: %d", messageTrackerSyncNumber)

	minSyncNumber := messageTrackerSyncNumber
	if osMetricsSyncNumber < minSyncNumber {
		minSyncNumber = osMetricsSyncNumber
	}

	if osMetricsSyncNumber > minSyncNumber {
		r.logger.Infof("Syncing os metrics from %d, because min is %d", osMetricsSyncNumber, minSyncNumber)
		err := r.osMetricsPersister.Rollback()
		if err != nil {
			r.logger.Errorf("Failed to rollback os metrics: %v", err)
			return nil, nil, 0, err
		}
		osMetricsMap, osMetricsSyncNumber = r.LoadOsMetrics(true)
		r.logger.Infof("Loaded backup os metrics with sync number: %d", osMetricsSyncNumber)
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

	if messageTrackerSyncNumber != osMetricsSyncNumber {
		return nil, nil, 0, fmt.Errorf("message tracker and os metrics sync numbers do not match: %d vs %d", messageTrackerSyncNumber, osMetricsSyncNumber)
	}

	return osMetricsMap, messageTracker, messageTrackerSyncNumber, nil
}
