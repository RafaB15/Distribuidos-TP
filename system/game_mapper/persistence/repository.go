package persistence

import (
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/persistence"
	"github.com/op/go-logging"
	"sync"
)

const (
	MessageTrackerFileName = "message_tracker"
)

type Repository struct {
	messageTrackerPersister *p.Persister[*n.MessageTracker]
	logger                  *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	messageTrackerPersister := p.NewPersister(MessageTrackerFileName, n.SerializeMessageTracker, n.DeserializeMessageTracker, wg, logger)

	return &Repository{
		messageTrackerPersister: messageTrackerPersister,
		logger:                  logger,
	}
}

func (r *Repository) SaveMessageTracker(messageTracker *n.MessageTracker, syncNumber uint64) error {
	return r.messageTrackerPersister.Save(messageTracker, syncNumber)
}

func (r *Repository) LoadMessageTracker(expectedEOFs int) (*n.MessageTracker, uint64) {
	messageTracker, syncNumber, err := r.messageTrackerPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load message tracker from file: %v. Returning new one", err)
		return n.NewMessageTracker(expectedEOFs), 0
	}
	return messageTracker, syncNumber
}
