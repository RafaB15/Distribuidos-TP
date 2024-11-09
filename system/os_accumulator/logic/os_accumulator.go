package os_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	rp "distribuidos-tp/system/os_accumulator/persistence"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type OSAccumulator struct {
	ReceiveGamesOS func() (int, []*oa.GameOS, bool, error)
	SendMetrics    func(int, *oa.GameOSMetrics) error
	SendEof        func(int) error
	Repository     *rp.Repository
}

func NewOSAccumulator(receiveGamesOS func() (int, []*oa.GameOS, bool, error), sendMetrics func(int, *oa.GameOSMetrics) error, sendEof func(int) error) *OSAccumulator {

	return &OSAccumulator{
		ReceiveGamesOS: receiveGamesOS,
		SendMetrics:    sendMetrics,
		SendEof:        sendEof,
		Repository:     rp.NewRepository("os_accumulator"),
	}
}

func (o *OSAccumulator) Run() error {
	for {

		clientID, gamesOS, eof, err := o.ReceiveGamesOS()
		if err != nil {
			log.Errorf("failed to receive game os: %v", err)
			return fmt.Errorf("failed to receive game os: %v", err)
		}

		clientOSMetrics, err := o.Repository.Load(clientID)
		if err != nil {
			log.Errorf("failed to load client %d os metrics: %v", clientID, err)
			continue
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
			continue
		}

		for _, gameOS := range gamesOS {
			clientOSMetrics.AddGameOS(gameOS)
		}

		o.Repository.Persist(clientID, *clientOSMetrics)
		log.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)

	}
}
