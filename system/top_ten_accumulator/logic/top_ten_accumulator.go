package top_ten_accumulator

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	rp "distribuidos-tp/system/top_ten_accumulator/persistence"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	AckBatchSize = 20
)

type TopTenAccumulator struct {
	ReceiveMsg     func(messageTracker *n.MessageTracker) (clientID int, gamesMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error)
	SendMsg        func(int, []*df.GameYearAndAvgPtf) error
	Repository     *rp.Repository
	AckLastMessage func() error
	logger         *logging.Logger
}

func NewTopTenAccumulator(receiveMsg func(messageTracker *n.MessageTracker) (clientID int, gamesMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error), sendMsg func(int, []*df.GameYearAndAvgPtf) error, ackLastMessage func() error, logger *logging.Logger) *TopTenAccumulator {
	return &TopTenAccumulator{
		ReceiveMsg:     receiveMsg,
		SendMsg:        sendMsg,
		Repository:     rp.NewRepository("top_ten_accumulator"),
		AckLastMessage: ackLastMessage,
		logger:         logger,
	}
}

func (t *TopTenAccumulator) Run(decadeFilterAmount int, fileName string) {
	messageTracker := n.NewMessageTracker(decadeFilterAmount)
	messagesUntilAck := AckBatchSize

	for {

		clientID, decadeGames, eof, newMessage, err := t.ReceiveMsg(messageTracker)
		if err != nil {
			log.Errorf("failed to receive message: %v", err)
			return
		}

		clientTopTenGames, err := t.Repository.Load(clientID)
		if err != nil {
			log.Errorf("failed to load client %d top ten games: %v", clientID, err)
			continue
		}

		if newMessage && !eof {
			clientTopTenGames = df.TopTenAvgPlaytimeForever(append(clientTopTenGames, decadeGames...))
			t.Repository.Persist(clientID, clientTopTenGames)
			// log.Infof("Updated top ten games for client %d", clientID)
		}

		if messageTracker.ClientFinished(clientID, log) {
			t.logger.Infof("Received all EOFs of client %d. Sending final metrics", clientID)
			err = t.SendMsg(clientID, clientTopTenGames)
			if err != nil {
				log.Errorf("failed to send metrics: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			messagesUntilAck = AckBatchSize
			err = t.AckLastMessage()
			if err != nil {
				t.logger.Errorf("Failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {
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
