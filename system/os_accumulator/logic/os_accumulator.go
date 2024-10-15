package os_accumulator

import (
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

func (o *OSAccumulator) Run() {
	osMetrics := oa.NewGameOSMetrics()

	for {
		gamesOS, eof, err := o.ReceiveGamesOS()
		if err != nil {
			log.Errorf("Failed to receive game os: %v", err)
			return
		}

		if eof {
			log.Infof("Received EOF. Sending metrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)
			err = o.SendMetrics(osMetrics)
			continue
		}

		for _, gameOS := range gamesOS {
			osMetrics.AddGameOS(gameOS)
		}

		log.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", osMetrics.Windows, osMetrics.Mac, osMetrics.Linux)
	}
}
