package decade_filter

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"

	"github.com/op/go-logging"
)

const DECADE = 2010

const (
	AckBatchSize = 100
)

var log = logging.MustGetLogger("log")

type DecadeFilter struct {
	ReceiveYearAvgPtf      func(messageTracker *n.MessageTracker) (int, []*df.GameYearAndAvgPtf, bool, bool, error)
	SendFilteredYearAvgPtf func(int, []*df.GameYearAndAvgPtf) error
	SendEof                func(clientID int, senderID int, messageTracker *n.MessageTracker) error
	AckLastMessage         func() error
	logger                 *logging.Logger
}

func NewDecadeFilter(receiveYearAvgPtf func(messageTracker *n.MessageTracker) (int, []*df.GameYearAndAvgPtf, bool, bool, error), sendFilteredYearAvgPtf func(int, []*df.GameYearAndAvgPtf) error, sendEof func(clientID int, senderID int, messageTracker *n.MessageTracker) error, ackLastMessage func() error, logger *logging.Logger) *DecadeFilter {
	return &DecadeFilter{
		ReceiveYearAvgPtf:      receiveYearAvgPtf,
		SendFilteredYearAvgPtf: sendFilteredYearAvgPtf,
		SendEof:                sendEof,
		AckLastMessage:         ackLastMessage,
		logger:                 logger,
	}
}

func (d *DecadeFilter) Run(senderID int) {
	messageTracker := n.NewMessageTracker(1)
	messagesUntilAck := AckBatchSize
	for {

		clientID, yearAvgPtfSlice, eof, newMessage, err := d.ReceiveYearAvgPtf(messageTracker)

		if err != nil {
			log.Errorf("failed to receive year and avg ptf: %v", err)
			return
		}

		if newMessage && !eof {

			yearsAvgPtfFiltered := df.FilterByDecade(yearAvgPtfSlice, DECADE)
			log.Infof("Sending ClientID %d filtered year and avg ptf to top ten accumulator", clientID)
			err = d.SendFilteredYearAvgPtf(clientID, yearsAvgPtfFiltered)
			if err != nil {
				log.Errorf("failed to send filtered year and avg ptf: %v", err)
				return
			}

		}

		if messageTracker.ClientFinished(clientID, d.logger) {
			log.Infof("Received client %d EOF. Sending EOF to top ten accumulator", clientID)
			err = d.SendEof(clientID, senderID, messageTracker)
			if err != nil {
				log.Errorf("failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			messagesUntilAck = AckBatchSize
			err = d.AckLastMessage()
			if err != nil {
				log.Errorf("failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {

			err = d.AckLastMessage()
			if err != nil {
				d.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}

	}
}
