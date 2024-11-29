package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	YearAvgPtfExchangeName     = "year_avg_ptf_exchange"
	YearAvgPtfExchangeType     = "direct"
	YearAvgPtfRoutingKeyPrefix = "year_avg_ptf_key_"
	YearAvgPtfQueueNamePrefix  = "year_avg_ptf_queue_"

	TopTenAccumulatorExchangeName = "top_ten_accumulator_exchange"
	TopTenAccumulatorExchangeType = "direct"
	TopTenAccumulatorRoutingKey   = "top_ten_accumulator_key"
)

type Middleware struct {
	Manager                   *mom.MiddlewareManager
	YearAvgPtfQueue           *mom.Queue    // Esta cola es para recibir del nodo anterior
	TopTenAccumulatorExchange *mom.Exchange // Este exchange es para el envío al siguiente nodo
	logger                    *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}
	yearAvgPtfQueueName := fmt.Sprintf("%s%d", YearAvgPtfQueueNamePrefix, id)
	yearAvgPtfRoutingKey := fmt.Sprintf("%s%d", YearAvgPtfRoutingKeyPrefix, id)

	yearAvgPtfQueue, err := manager.CreateBoundQueue(yearAvgPtfQueueName, YearAvgPtfExchangeName, YearAvgPtfExchangeType, yearAvgPtfRoutingKey, false)
	if err != nil {
		return nil, err
	}

	topTenAccumulatorExchange, err := manager.CreateExchange(TopTenAccumulatorExchangeName, TopTenAccumulatorExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                   manager,
		YearAvgPtfQueue:           yearAvgPtfQueue,
		TopTenAccumulatorExchange: topTenAccumulatorExchange,
		logger:                    logger,
	}, nil
}

func (m *Middleware) ReceiveYearAvgPtf(messageTracker *n.MessageTracker) (clientID int, gamesMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error) {

	rawMsg, err := m.YearAvgPtfQueue.Consume()
	if err != nil {
		return 0, nil, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)

	if err != nil {
		return 0, nil, false, false, err
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, false, false, err
	}

	if !newMessage {
		return 0, nil, false, false, nil
	}

	switch message.Type {

	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to deserialize EOF: %v", err)
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}

		return message.ClientID, nil, true, true, nil
	case sp.MsgGameYearAndAvgPtfInformation:
		gamesYearsAvgPtfs, err := sp.DeserializeMsgGameYearAndAvgPtf(message.Body)

		if err != nil {
			return message.ClientID, nil, false, false, err
		}

		return message.ClientID, gamesYearsAvgPtfs, false, true, nil
	default:
		return message.ClientID, nil, false, false, nil
	}

}

func (m *Middleware) SendFilteredYearAvgPtf(clientID int, gamesYearsAvgPtfs []*df.GameYearAndAvgPtf, messageTracker *n.MessageTracker) error {
	data := sp.SerializeMsgGameYearAndAvgPtf(clientID, gamesYearsAvgPtfs)

	fmt.Printf("About to publish to top ten accumulator exchange\n")
	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, data)
	if err != nil {
		return err
	}
	fmt.Printf("Published to top ten accumulator exchange\n")

	messageTracker.RegisterSentMessage(clientID, TopTenAccumulatorRoutingKey)

	return nil
}

func (m *Middleware) SendEof(clientID int, senderID int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)
	messagesSentToNode := messagesSent[TopTenAccumulatorRoutingKey]
	serializedMessage := sp.SerializeMsgEndOfFileV2(clientID, senderID, messagesSentToNode)
	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, serializedMessage)
	if err != nil {
		return err
	}
	m.logger.Infof("Sent EOF to client %d", clientID)
	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.YearAvgPtfQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
