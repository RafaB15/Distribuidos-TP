package os_accumulator

import (
	"context"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSAccumulator struct {
	ReceiveGamesOS func() (int, []*oa.GameOS, bool, error)
	SendMetrics    func(int, *oa.GameOSMetrics) error
	SendEof        func(int) error
}

func NewOSAccumulator(receiveGamesOS func() (int, []*oa.GameOS, bool, error), sendMetrics func(int, *oa.GameOSMetrics) error, sendEof func(int) error) *OSAccumulator {
	return &OSAccumulator{
		ReceiveGamesOS: receiveGamesOS,
		SendMetrics:    sendMetrics,
		SendEof:        sendEof,
	}
}

func (o *OSAccumulator) Run(ctx context.Context) error {
	osMetricsMap := make(map[int]*oa.GameOSMetrics)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
			clientID, gamesOS, eof, err := o.ReceiveGamesOS()
			if err != nil {
				log.Errorf("failed to receive game os: %v", err)
				return fmt.Errorf("failed to receive game os: %v", err)
			}

			clientOSMetrics, ok := osMetricsMap[clientID]

			if !ok {
				osMetricsMap[clientID] = oa.NewGameOSMetrics()
				clientOSMetrics = osMetricsMap[clientID]
			}

			if eof {
				log.Infof("Received EOF. Sending metrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
				err = o.SendMetrics(clientID, clientOSMetrics)
				if err != nil {
					log.Errorf("failed to send metrics: %v", err)
				}

				err = o.SendEof(clientID)
				if err != nil {
					log.Errorf("failed to send EOF: %v", err)
				}
				delete(osMetricsMap, clientID)
				continue
			}

			for _, gameOS := range gamesOS {
				clientOSMetrics.AddGameOS(gameOS)
			}

			log.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
		}
	}
}
