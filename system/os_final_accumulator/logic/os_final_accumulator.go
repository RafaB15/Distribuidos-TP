package os_final_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	"github.com/op/go-logging"
)

const (
	AckBatchSize = 20
)

type ReceiveGamesOSMetricsFunc func(messageTracker *n.MessageTracker) (clientID int, gamesOS *oa.GameOSMetrics, eof bool, newMessage bool, err error)
type SendFinalMetricsFunc func(int, *oa.GameOSMetrics) error
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

func (o *OSFinalAccumulator) Run(osAccumulatorsAmount int) {
	osMetricsMap := make(map[int]*oa.GameOSMetrics)
	messageTracker := n.NewMessageTracker(osAccumulatorsAmount)

	messageUntilAck := AckBatchSize

	for {
		clientID, gamesOSMetrics, eof, newMessage, err := o.ReceiveGamesOSMetrics(messageTracker)
		if err != nil {
			o.logger.Errorf("failed to receive game os metrics: %v", err)
			return
		}

		clientOSMetrics, exists := osMetricsMap[clientID]
		if !exists {
			clientOSMetrics = &oa.GameOSMetrics{}
			osMetricsMap[clientID] = clientOSMetrics
		}

		if newMessage && !eof {
			clientOSMetrics.Merge(gamesOSMetrics)
			o.logger.Infof("Received Game Os Metrics Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
		}

		if messageTracker.ClientFinished(clientID, o.logger) {
			o.logger.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
			err = o.SendFinalMetrics(clientID, clientOSMetrics)
			if err != nil {
				o.logger.Errorf("Failed to send final metrics: %v", err)
				return
			}
			delete(osMetricsMap, clientID)
			messageTracker.DeleteClientInfo(clientID)

			messageUntilAck = AckBatchSize
			err = o.AckLastMessage()
			if err != nil {
				o.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messageUntilAck == 0 {
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
