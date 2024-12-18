package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSGamesExchangeName     = "os_games_exchange"
	OSGamesExchangeType     = "direct"
	OSGamesRoutingKeyPrefix = "os_games_key_"
	OSGamesQueueNamePrefix  = "os_games_queue_"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
)

type Middleware struct {
	Manager               *mom.MiddlewareManager
	OSGamesQueue          *mom.Queue
	OSAccumulatorExchange *mom.Exchange
	logger                *logging.Logger
}

func NewMiddleware(id int, logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	osGamesQueueName := fmt.Sprintf("%s%d", OSGamesQueueNamePrefix, id)
	osGamesRoutingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, id)

	osGamesQueue, err := manager.CreateBoundQueue(osGamesQueueName, OSGamesExchangeName, OSGamesExchangeType, osGamesRoutingKey, false)
	if err != nil {
		return nil, err
	}

	osAccumulatorExchange, err := manager.CreateExchange(OSAccumulatorExchangeName, OSAccumulatorExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:               manager,
		OSGamesQueue:          osGamesQueue,
		OSAccumulatorExchange: osAccumulatorExchange,
		logger:                logger,
	}, nil
}

func (m *Middleware) SendMetrics(clientID int, gameMetrics *oa.GameOSMetrics, messageTracker *n.MessageTracker) error {
	data := sp.SerializeGameOSMetrics(clientID, gameMetrics)
	err := m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, data)
	if err != nil {
		return err
	}

	messageTracker.RegisterSentMessage(clientID, OSAccumulatorRoutingKey)

	return nil
}

func (m *Middleware) ReceiveGameOS(messageTracker *n.MessageTracker) (clientID int, gamesOS []*oa.GameOS, eof bool, newMessage bool, delMessage bool, err error) {
	rawMsg, err := m.OSGamesQueue.Consume()
	if err != nil {
		return 0, nil, false, false, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, false, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		return 0, nil, false, false, false, fmt.Errorf("failed to process message: %v", err)
	}

	if !newMessage {
		return message.ClientID, nil, false, false, false, nil
	}

	switch message.Type {

	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to deserialize EOF: %v", err)
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			return message.ClientID, nil, false, false, false, fmt.Errorf("failed to register EOF: %v", err)
		}

		return message.ClientID, nil, true, true, false, nil
	case sp.MsgDeleteClient:
		m.logger.Infof("Received delete client from client %d", message.ClientID)
		return message.ClientID, nil, false, true, true, nil
	case sp.MsgGameOSInformation:
		gamesOs, err := sp.DeserializeMsgGameOSInformation(message.Body)

		if err != nil {
			return message.ClientID, nil, false, true, false, err
		}

		return message.ClientID, gamesOs, false, true, false, nil
	default:
		return message.ClientID, nil, false, false, false, fmt.Errorf("unexpected message type: %v", message.Type)
	}
}

func (m *Middleware) SendEof(clientID int, senderID int, messageTracker *n.MessageTracker) error {
	messagesSent := messageTracker.GetSentMessages(clientID)
	messagesSentToNode := messagesSent[OSAccumulatorRoutingKey]
	serializedMessage := sp.SerializeMsgEndOfFile(clientID, senderID, messagesSentToNode)
	err := m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, serializedMessage)
	if err != nil {
		return err
	}
	m.logger.Infof("Sent EOF to client %d", clientID)

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.OSGamesQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}

func (m *Middleware) SendDeleteClient(clientID int) error {
	serializedMessage := sp.SerializeMsgDeleteClient(clientID)
	err := m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, serializedMessage)
	if err != nil {
		return err
	}
	m.logger.Infof("Sent delete client to Final accumulator for client %d", clientID)

	return nil
}
