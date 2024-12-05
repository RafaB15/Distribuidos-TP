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

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
	OSAccumulatorQueueName    = "os_accumulator_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	OSAccumulatorQueue *mom.Queue
	QueryExchange      *mom.Exchange
	logger             *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	osAccumulatorQueue, err := manager.CreateBoundQueue(OSAccumulatorQueueName, OSAccumulatorExchangeName, OSAccumulatorExchangeType, OSAccumulatorRoutingKey, false)
	if err != nil {
		return nil, err
	}

	queryExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:            manager,
		OSAccumulatorQueue: osAccumulatorQueue,
		QueryExchange:      queryExchange,
		logger:             logger,
	}, nil
}

func (m *Middleware) ReceiveGamesOSMetrics(messageTracker *n.MessageTracker) (clientID int, gamesOS *oa.GameOSMetrics, eof bool, newMessage bool, delMessage bool, e error) {
	rawMsg, err := m.OSAccumulatorQueue.Consume()
	if err != nil {
		e = fmt.Errorf("failed to consume message: %v", err)
		return
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		e = fmt.Errorf("failed to deserialize message: %v", err)
		return
	}

	delMessage = false
	clientID = message.ClientID

	newMessage, err = messageTracker.ProcessMessage(message.ClientID, message.Body)
	if err != nil {
		e = fmt.Errorf("failed to process message: %v", err)
		return
	}

	if !newMessage {
		return
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		m.logger.Infof("Received EOF message from client %d", message.ClientID)
		endOfFile, err := sp.DeserializeMsgEndOfFile(message.Body)
		if err != nil {
			e = fmt.Errorf("failed to deserialize EOF: %v", err)
			return
		}

		err = messageTracker.RegisterEOF(message.ClientID, endOfFile, m.logger)
		if err != nil {
			e = fmt.Errorf("failed to register EOF: %v", err)
			return
		}
		eof = true
	case sp.MsgDeleteClient:
		m.logger.Infof("Received Delete Client message for client %d", message.ClientID)
		delMessage = true
		return
	case sp.MsgAccumulatedGameOSInformation:
		gamesOS, e = sp.DeserializeMsgAccumulatedGameOSInformationV2(message.Body)
	default:
		e = fmt.Errorf("received unexpected message type: %v", message.Type)
	}
	return
}

func (m *Middleware) SendFinalMetrics(clientID int, gameMetrics *oa.GameOSMetrics) error {
	queryMessage := sp.SerializeMsgOsResolvedQuery(clientID, gameMetrics)

	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	err := m.QueryExchange.Publish(routingKey, queryMessage)
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.OSAccumulatorQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
