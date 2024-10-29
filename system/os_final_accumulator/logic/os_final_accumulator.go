package os_final_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSFinalAccumulator struct {
	ReceiveGamesOSMetrics func() (int, *oa.GameOSMetrics, bool, error)
	SendFinalMetrics      func(int, *oa.GameOSMetrics) error
	OSAccumulatorsAmount  int
}

func NewOSFinalAccumulator(receiveGamesOSMetrics func() (int, *oa.GameOSMetrics, bool, error), sendFinalMetrics func(int, *oa.GameOSMetrics) error, osAccumulatorsAmount int) *OSFinalAccumulator {
	return &OSFinalAccumulator{
		ReceiveGamesOSMetrics: receiveGamesOSMetrics,
		SendFinalMetrics:      sendFinalMetrics,
		OSAccumulatorsAmount:  osAccumulatorsAmount,
	}
}

func (o *OSFinalAccumulator) Run() error {
	osMetricsMap := make(map[int]*oa.GameOSMetrics)
	remainingEOFsMap := make(map[int]int)

	for {
		clientID, gamesOSMetrics, eof, err := o.ReceiveGamesOSMetrics()
		if err != nil {
			return fmt.Errorf("failed to receive game os metrics: %v", err)
		}

		if eof {
			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = o.OSAccumulatorsAmount
			}

			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs

			if remainingEOFsMap[clientID] <= 0 {
				log.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
				err = o.SendFinalMetrics(clientID, osMetricsMap[clientID])
				if err != nil {
					// log.Errorf("Failed to send final metrics: %v", err)
					return fmt.Errorf("failed to send final metrics: %v", err)
				}
				delete(osMetricsMap, clientID)
				delete(remainingEOFsMap, clientID)
			}
			continue
		}

		if _, ok := osMetricsMap[clientID]; !ok {
			osMetricsMap[clientID] = oa.NewGameOSMetrics()
		}

		osMetrics := osMetricsMap[clientID]

		osMetrics.Merge(gamesOSMetrics)
		log.Infof("Received Game Os Metrics Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)

	}
}
