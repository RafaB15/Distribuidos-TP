package decade_filter

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/decade_filter/persistence"
	"math/rand"

	"github.com/op/go-logging"
)

const DECADE = 2010

const (
	AckBatchSize = 10
)

var log = logging.MustGetLogger("log")

type DecadeFilter struct {
	ReceiveYearAvgPtf      func(messageTracker *n.MessageTracker) (int, []*df.GameYearAndAvgPtf, bool, bool, error)
	SendFilteredYearAvgPtf func(clientID int, gameMetrics []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error
	SendEof                func(clientID int, senderID int, messageTracker *n.MessageTracker) error
	AckLastMessage         func() error
	logger                 *logging.Logger
}

func NewDecadeFilter(receiveYearAvgPtf func(messageTracker *n.MessageTracker) (int, []*df.GameYearAndAvgPtf, bool, bool, error), sendFilteredYearAvgPtf func(clientID int, gameMetrics []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error, sendEof func(clientID int, senderID int, messageTracker *n.MessageTracker) error, ackLastMessage func() error, logger *logging.Logger) *DecadeFilter {
	return &DecadeFilter{
		ReceiveYearAvgPtf:      receiveYearAvgPtf,
		SendFilteredYearAvgPtf: sendFilteredYearAvgPtf,
		SendEof:                sendEof,
		AckLastMessage:         ackLastMessage,
		logger:                 logger,
	}
}

func (d *DecadeFilter) Run(senderID int, repository *p.Repository) {
	messageTracker, syncNumber := repository.LoadMessageTracker(1)
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

			if len(yearsAvgPtfFiltered) > 0 {
				err = d.SendFilteredYearAvgPtf(clientID, yearsAvgPtfFiltered, messageTracker)
				if err != nil {
					log.Errorf("failed to send filtered year and avg ptf: %v", err)
					return
				}
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

			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				d.logger.Errorf("Failed to save message tracker: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = d.AckLastMessage()
			if err != nil {
				log.Errorf("failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {
			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				d.logger.Errorf("Failed to save message tracker: %v", err)
				return
			}

			if rand.Float32() < 0.099 {
				d.logger.Infof("Simulated crash")
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
