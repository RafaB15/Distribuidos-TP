package decade_filter

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/decade_filter/persistence"

	"github.com/op/go-logging"
)

const DECADE = 2010

const (
	AckBatchSize = 10
)

type ReceiveYearAvgPtfFunc func(messageTracker *n.MessageTracker) (int, []*df.GameYearAndAvgPtf, bool, bool, bool, error)
type SendFilteredYearAvgPtfFunc func(clientID int, gameMetrics []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error
type SendEofFunc func(clientID int, senderID int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int) error
type AckLastMessageFunc func() error

type DecadeFilter struct {
	ReceiveYearAvgPtf      ReceiveYearAvgPtfFunc
	SendFilteredYearAvgPtf SendFilteredYearAvgPtfFunc
	SendEof                SendEofFunc
	SendDeleteClient       SendDeleteClientFunc
	AckLastMessage         AckLastMessageFunc
	logger                 *logging.Logger
}

func NewDecadeFilter(
	receiveYearAvgPtf ReceiveYearAvgPtfFunc,
	sendFilteredYearAvgPtf SendFilteredYearAvgPtfFunc,
	sendEof SendEofFunc,
	sendDeleteClient SendDeleteClientFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *DecadeFilter {
	return &DecadeFilter{
		ReceiveYearAvgPtf:      receiveYearAvgPtf,
		SendFilteredYearAvgPtf: sendFilteredYearAvgPtf,
		SendEof:                sendEof,
		SendDeleteClient:       sendDeleteClient,
		AckLastMessage:         ackLastMessage,
		logger:                 logger,
	}
}

func (d *DecadeFilter) Run(senderID int, repository *p.Repository) {
	messageTracker, syncNumber := repository.LoadMessageTracker(1)
	messagesUntilAck := AckBatchSize
	for {

		clientID, yearAvgPtfSlice, eof, newMessage, delMessage, err := d.ReceiveYearAvgPtf(messageTracker)

		if err != nil {
			d.logger.Errorf("failed to receive year and avg ptf: %v", err)
			return
		}

		if newMessage && !eof && !delMessage {

			yearsAvgPtfFiltered := df.FilterByDecade(yearAvgPtfSlice, DECADE)

			if len(yearsAvgPtfFiltered) > 0 {
				err = d.SendFilteredYearAvgPtf(clientID, yearsAvgPtfFiltered, messageTracker)
				if err != nil {
					d.logger.Errorf("failed to send filtered year and avg ptf: %v", err)
					return
				}
			}

		}

		if delMessage {
			d.logger.Infof("Deleting client %d information", clientID)
			err = d.SendDeleteClient(clientID)

			messageTracker.DeleteClientInfo(clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, d.logger)
		if clientFinished {
			d.logger.Infof("Received client %d EOF. Sending EOF to top ten accumulator", clientID)
			err = d.SendEof(clientID, senderID, messageTracker)
			if err != nil {
				d.logger.Errorf("failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
		}

		if messagesUntilAck == 0 || delMessage || clientFinished {
			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				d.logger.Errorf("Failed to save message tracker: %v", err)
				return
			}

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
