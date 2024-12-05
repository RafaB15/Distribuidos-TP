package persistence

import (
	e "distribuidos-tp/internal/system_protocol/entrypoint"
	p "distribuidos-tp/persistence"
	"github.com/op/go-logging"
	"sync"
)

const (
	ClientTrackerFileName = "client_tracker"
)

type Repository struct {
	clientTrackerPersister *p.Persister[*e.ClientTracker]
	logger                 *logging.Logger
}

func NewRepository(wg *sync.WaitGroup, logger *logging.Logger) *Repository {
	clientTrackerPersister := p.NewPersister(ClientTrackerFileName, e.SerializeClientTracker, e.DeserializeClientTracker, wg, logger)

	return &Repository{
		clientTrackerPersister: clientTrackerPersister,
		logger:                 logger,
	}
}

func (r *Repository) SaveClientTracker(clientTracker *e.ClientTracker) error {
	return r.clientTrackerPersister.Save(clientTracker, 0)
}

func (r *Repository) LoadClientTracker() *e.ClientTracker {
	clientTracker, _, err := r.clientTrackerPersister.Load()
	if err != nil {
		r.logger.Errorf("Failed to load client tracker from file: %v. Returning new one", err)
		return e.NewClientTracker()
	}
	return clientTracker
}
