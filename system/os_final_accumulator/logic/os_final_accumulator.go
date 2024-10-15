package os_final_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSFinalAccumulator struct {
	ReceiveGamesOSMetrics func() (*oa.GameOSMetrics, error)
	SendFinalMetrics      func(*oa.GameOSMetrics) error
	OSAccumulatorsAmount  int
}

func NewOSFinalAccumulator(receiveGamesOSMetrics func() (*oa.GameOSMetrics, error), sendFinalMetrics func(*oa.GameOSMetrics) error, osAccumulatorsAmount int) *OSFinalAccumulator {
	return &OSFinalAccumulator{
		ReceiveGamesOSMetrics: receiveGamesOSMetrics,
		SendFinalMetrics:      sendFinalMetrics,
		OSAccumulatorsAmount:  osAccumulatorsAmount,
	}
}

func (o *OSFinalAccumulator) Run() {
	nodesLeft := o.OSAccumulatorsAmount

	osMetrics := oa.NewGameOSMetrics()

	for {
		gamesOSMetrics, err := o.ReceiveGamesOSMetrics()
		if err != nil {
			log.Errorf("Failed to receive game os metrics: %v", err)
			return
		}
		osMetrics.Merge(gamesOSMetrics)
		log.Infof("Received Game Os Metrics Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)

		nodesLeft--
		if nodesLeft > 0 {
			continue
		}
		log.Infof("Sending final metrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)
		err = o.SendFinalMetrics(osMetrics)
		if err != nil {
			log.Errorf("Failed to send final metrics: %v", err)
			return
		}
		return
	}
}
