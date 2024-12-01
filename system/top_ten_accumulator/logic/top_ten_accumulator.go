package top_ten_accumulator

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/top_ten_accumulator/persistence"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	AckBatchSize = 20
)

type TopTenAccumulator struct {
	ReceiveMsg     func(messageTracker *n.MessageTracker) (clientID int, gamesMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error)
	SendMsg        func(int, []*df.GameYearAndAvgPtf) error
	AckLastMessage func() error
	logger         *logging.Logger
}

func NewTopTenAccumulator(receiveMsg func(messageTracker *n.MessageTracker) (clientID int, gamesMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error), sendMsg func(int, []*df.GameYearAndAvgPtf) error, ackLastMessage func() error, logger *logging.Logger) *TopTenAccumulator {
	return &TopTenAccumulator{
		ReceiveMsg:     receiveMsg,
		SendMsg:        sendMsg,
		AckLastMessage: ackLastMessage,
		logger:         logger,
	}
}

func (t *TopTenAccumulator) Run(decadeFilterAmount int, repository *p.Repository) {
	topTenGamesMap, messageTracker, syncNumber, err := repository.LoadAll(decadeFilterAmount)
	if err != nil {
		t.logger.Errorf("failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {

		clientID, decadeGames, eof, newMessage, err := t.ReceiveMsg(messageTracker)
		if err != nil {
			log.Errorf("failed to receive message: %v", err)
			return
		}

		clientTopTenGames, exists := topTenGamesMap.Get(clientID)
		if !exists {
			clientTopTenGames = []*df.GameYearAndAvgPtf{}
			topTenGamesMap.Set(clientID, clientTopTenGames)
		}

		if newMessage && !eof {
			clientTopTenGames = df.TopTenAvgPlaytimeForever(append(clientTopTenGames, decadeGames...))
			topTenGamesMap.Set(clientID, clientTopTenGames)
		}

		if messageTracker.ClientFinished(clientID, log) {
			t.logger.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
			err = t.SendMsg(clientID, clientTopTenGames)
			if err != nil {
				log.Errorf("failed to send metrics: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
			topTenGamesMap.Delete(clientID)
			syncNumber++

			err = repository.SaveAll(topTenGamesMap, messageTracker, syncNumber)
			if err != nil {
				log.Errorf("failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = t.AckLastMessage()
			if err != nil {
				t.logger.Errorf("Failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {

			syncNumber++
			err = repository.SaveAll(topTenGamesMap, messageTracker, syncNumber)
			if err != nil {
				log.Errorf("failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = t.AckLastMessage()
			if err != nil {
				t.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		} else {
			messagesUntilAck--
		}

	}
}
