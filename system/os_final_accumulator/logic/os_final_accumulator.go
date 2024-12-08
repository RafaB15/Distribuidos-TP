package os_final_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/os_final_accumulator/persistence"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 20
)

type ReceiveGamesOSMetricsFunc func(messageTracker *n.MessageTracker) (clientID int, gamesOS *oa.GameOSMetrics, eof bool, newMessage bool, delMessage bool, err error)
type SendFinalMetricsFunc func(clientID int, games *oa.GameOSMetrics) error
type AckLastMessageFunc func() error

type OSFinalAccumulator struct {
	ReceiveGamesOSMetrics ReceiveGamesOSMetricsFunc
	SendFinalMetrics      SendFinalMetricsFunc
	AckLastMessage        AckLastMessageFunc
	logger                *logging.Logger
}

func NewOSFinalAccumulator(
	receiveGamesOSMetrics ReceiveGamesOSMetricsFunc,
	sendFinalMetrics SendFinalMetricsFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *OSFinalAccumulator {
	return &OSFinalAccumulator{
		ReceiveGamesOSMetrics: receiveGamesOSMetrics,
		SendFinalMetrics:      sendFinalMetrics,
		AckLastMessage:        ackLastMessage,
		logger:                logger,
	}
}

func (o *OSFinalAccumulator) Run(osAccumulatorsAmount int, repository *p.Repository) {

	osMetricsMap, messageTracker, syncNumber, err := repository.LoadAll(osAccumulatorsAmount)
	if err != nil {
		o.logger.Errorf("failed to load data: %v", err)
		return
	}

	messageUntilAck := AckBatchSize

	for {
		clientID, gamesOSMetrics, eof, newMessage, delMessage, err := o.ReceiveGamesOSMetrics(messageTracker)
		if err != nil {
			o.logger.Errorf("failed to receive game os metrics: %v", err)
			return
		}

		clientOSMetrics, exists := osMetricsMap.Get(clientID)
		if !exists {
			clientOSMetrics = &oa.GameOSMetrics{}
			osMetricsMap.Set(clientID, clientOSMetrics)
		}

		if newMessage && !eof && !delMessage {
			clientOSMetrics.Merge(gamesOSMetrics)
			o.logger.Infof("Received Game Os Metrics Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
		}

		if delMessage {
			o.logger.Infof("Received Delete Client Message. Deleting client %d", clientID)
			messageTracker.DeleteClientInfo(clientID)
			osMetricsMap.Delete(clientID)

			o.logger.Infof("Deleted all client %d information", clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, o.logger)
		if clientFinished {

			o.logger.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
			err = o.SendFinalMetrics(clientID, clientOSMetrics)
			if err != nil {
				o.logger.Errorf("Failed to send final metrics: %v", err)
				return
			}
			messageTracker.DeleteClientInfo(clientID)
			osMetricsMap.Delete(clientID)
		}

		if messageUntilAck == 0 || delMessage || clientFinished {
			saves := 1
			if delMessage || clientFinished {
				saves = 2
			}

			for i := 0; i < saves; i++ {
				syncNumber++
				err = repository.SaveAll(osMetricsMap, messageTracker, syncNumber)
				if err != nil {
					o.logger.Errorf("failed to save data: %v", err)
					return
				}
			}

			messageUntilAck = AckBatchSize
			err = o.AckLastMessage()
			if err != nil {
				o.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		} else {
			messageUntilAck--
		}
	}
}
