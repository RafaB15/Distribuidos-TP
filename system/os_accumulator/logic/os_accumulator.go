package os_accumulator

import (
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/os_accumulator/persistence"

	"github.com/op/go-logging"
)

const (
	GameMapperAmount = 1
	AckBatchSize     = 20
)

type ReceiveGamesOSFunc func(messageTracker *n.MessageTracker) (clientID int, gamesOS []*oa.GameOS, eof bool, newMessage bool, delMessage bool, err error)
type SendMetricsFunc func(clientID int, gameMetrics *oa.GameOSMetrics, messageTracker *n.MessageTracker) error
type SendEofFunc func(clientID int, senderID int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int) error
type AckLastMessageFunc func() error

type OSAccumulator struct {
	ReceiveGamesOS   ReceiveGamesOSFunc
	SendMetrics      SendMetricsFunc
	SendEof          SendEofFunc
	SendDeleteClient SendDeleteClientFunc
	AckLastMessage   AckLastMessageFunc
	logger           *logging.Logger
}

func NewOSAccumulator(
	receiveGamesOS ReceiveGamesOSFunc,
	sendMetrics SendMetricsFunc,
	sendEof SendEofFunc,
	sendDeleteClient SendDeleteClientFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *OSAccumulator {
	return &OSAccumulator{
		ReceiveGamesOS:   receiveGamesOS,
		SendMetrics:      sendMetrics,
		SendEof:          sendEof,
		SendDeleteClient: sendDeleteClient,
		AckLastMessage:   ackLastMessage,
		logger:           logger,
	}
}

func (o *OSAccumulator) Run(id int, repository *p.Repository) {
	osMetricsMap, messageTracker, syncNumber, err := repository.LoadAll(GameMapperAmount)
	if err != nil {
		o.logger.Errorf("failed to load data: %v", err)
		return
	}

	messageUntilAck := AckBatchSize

	for {

		clientID, gamesOS, eof, newMessage, delMessage, err := o.ReceiveGamesOS(messageTracker)
		if err != nil {
			o.logger.Errorf("failed to receive game os: %v", err)
			return
		}

		clientOSMetrics, exists := osMetricsMap.Get(clientID)
		if !exists {
			clientOSMetrics = oa.NewGameOSMetrics()
			osMetricsMap.Set(clientID, clientOSMetrics)
		}

		if newMessage && !eof && !delMessage {
			for _, gameOS := range gamesOS {
				clientOSMetrics.AddGameOS(gameOS)
			}
			o.logger.Infof("Received Game Os Information. Updated osMetrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
		}

		if delMessage {
			o.logger.Infof("Received Delete Message for client: %v", clientID)
			err = o.SendDeleteClient(clientID)
			if err != nil {
				o.logger.Errorf("failed to send delete message: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
			osMetricsMap.Delete(clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, o.logger)
		if clientFinished {
			o.logger.Infof("Received EOF. Sending metrics: Windows: %v, Mac: %v, Linux: %v", clientOSMetrics.Windows, clientOSMetrics.Mac, clientOSMetrics.Linux)
			err = o.SendMetrics(clientID, clientOSMetrics, messageTracker)
			if err != nil {
				o.logger.Errorf("failed to send metrics: %v", err)
				return
			}

			err = o.SendEof(clientID, id, messageTracker)
			if err != nil {
				o.logger.Errorf("failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
			osMetricsMap.Delete(clientID)
		}

		if messageUntilAck == 0 || delMessage || clientFinished {
			syncNumber++
			err = repository.SaveAll(osMetricsMap, messageTracker, syncNumber)
			if err != nil {
				o.logger.Errorf("failed to save data: %v", err)
				return
			}

			err = o.AckLastMessage()
			if err != nil {
				o.logger.Errorf("failed to ack last message: %v", err)
				return
			}
			messageUntilAck = AckBatchSize
		} else {
			messageUntilAck--
		}
	}
}
