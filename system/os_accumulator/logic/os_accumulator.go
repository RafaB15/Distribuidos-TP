package os_accumulator

import (
	"context"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSAccumulator struct {
	ReceiveGamesOS func() ([]*oa.GameOS, bool, error)
	SendMetrics    func(*oa.GameOSMetrics) error
}

func NewOSAccumulator(receiveGamesOS func() ([]*oa.GameOS, bool, error), sendMetrics func(*oa.GameOSMetrics) error) *OSAccumulator {
	return &OSAccumulator{
		ReceiveGamesOS: receiveGamesOS,
		SendMetrics:    sendMetrics,
	}
}

func (o *OSAccumulator) Run(ctx context.Context) {
	osMetrics := oa.NewGameOSMetrics()

	for {
		select {
		case <-ctx.Done():
			log.Info("Context canceled, graceful shutdown.")
			return
		default:
			gamesOS, eof, err := o.ReceiveGamesOS()
			if err != nil {
				log.Errorf("Failed to receive game os: %v", err)
				return
			}

			if eof {
				log.Infof("Received EOF. Sending metrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)
				err = o.SendMetrics(osMetrics)
				if err != nil {
					log.Errorf("Failed to send metrics: %v", err)
				}
				continue
			}

			for _, gameOS := range gamesOS {
				osMetrics.AddGameOS(gameOS)
			}

			log.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)
		}
	}
}
