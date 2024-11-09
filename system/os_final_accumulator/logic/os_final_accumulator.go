package os_final_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	rp "distribuidos-tp/system/os_final_accumulator/persistence"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSFinalAccumulator struct {
	ReceiveGamesOSMetrics func() (int, *oa.GameOSMetrics, bool, error)
	SendFinalMetrics      func(int, *oa.GameOSMetrics) error
	OSAccumulatorsAmount  int
	Repository            *rp.Repository
}

func NewOSFinalAccumulator(receiveGamesOSMetrics func() (int, *oa.GameOSMetrics, bool, error), sendFinalMetrics func(int, *oa.GameOSMetrics) error, osAccumulatorsAmount int) *OSFinalAccumulator {
	return &OSFinalAccumulator{
		ReceiveGamesOSMetrics: receiveGamesOSMetrics,
		SendFinalMetrics:      sendFinalMetrics,
		OSAccumulatorsAmount:  osAccumulatorsAmount,
		Repository:            rp.NewRepository("os_final_accumulator"),
	}
}

func (o *OSFinalAccumulator) Run() error {

	for {
		clientID, gamesOSMetrics, eof, err := o.ReceiveGamesOSMetrics()
		if err != nil {
			return fmt.Errorf("failed to receive game os metrics: %v", err)
		}

		osMetrics, err := o.Repository.Load(clientID)
		if err != nil {
			log.Errorf("failed to load client %d os metrics: %v", clientID, err)
			continue
		}

		if eof {

			remainingEOFs, err := o.Repository.PersistAndUpdateEof(clientID, o.OSAccumulatorsAmount)
			if err != nil {
				return fmt.Errorf("failed to persist and update eof: %v", err)
			}

			if remainingEOFs <= 0 {
				log.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
				err = o.SendFinalMetrics(clientID, osMetrics)
				if err != nil {
					log.Errorf("Failed to send final metrics: %v", err)
					return fmt.Errorf("failed to send final metrics: %v", err)
				}

			}
			continue
		}

		osMetrics.Merge(gamesOSMetrics)
		o.Repository.Persist(clientID, *osMetrics)
		log.Infof("Received Game Os Metrics Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)

	}
}
