package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	n "distribuidos-tp/internal/system_protocol/node"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopTenAccumulatorExchangeName = "top_ten_accumulator_exchange"
	TopTenAccumulatorExchangeType = "direct"
	TopTenAccumulatorRoutingKey   = "top_ten_accumulator_key"
	TopTenAccumulatorQueueName    = "top_ten_accumulator_queue"

	QueryResultsExchangeName = "query_results_exchange"
	QueryRoutingKeyPrefix    = "query_results_key_" // con el id del cliente
	QueryExchangeType        = "direct"
)

type Middleware struct {
	Manager                *mom.MiddlewareManager
	TopTenAccumulatorQueue *mom.Queue
	WriterExchange         *mom.Exchange
	logger                 *logging.Logger
}

func NewMiddleware(logger *logging.Logger) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	topTenAccumulatorQueue, err := manager.CreateBoundQueue(TopTenAccumulatorQueueName, TopTenAccumulatorExchangeName, TopTenAccumulatorExchangeType, TopTenAccumulatorRoutingKey, false)
	if err != nil {
		return nil, err
	}

	writerExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                manager,
		TopTenAccumulatorQueue: topTenAccumulatorQueue,
		WriterExchange:         writerExchange,
		logger:                 logger,
	}, nil
}

func (m *Middleware) ReceiveMsg(messageTracker *n.MessageTracker) (clientID int, gameMetrics []*df.GameYearAndAvgPtf, eof bool, newMessage bool, e error) {
	rawMsg, err := m.TopTenAccumulatorQueue.Consume()
	if err != nil {
		e = fmt.Errorf("failed to consume message: %v", err)
		return
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		e = fmt.Errorf("failed to deserialize message: %v", err)
		return
	}
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

	case sp.MsgGameYearAndAvgPtfInformation:
		gameMetrics, e = sp.DeserializeMsgGameYearAndAvgPtf(message.Body)
		// printMetrics(message)

	default:
		e = fmt.Errorf("received unexpected message type: %v", message.Type)
	}
	return
}

func (m *Middleware) SendMsg(clientID int, finalTopTenGames []*df.GameYearAndAvgPtf) error {

	queryMessage := sp.SerializeMsgTopTenResolvedQuery(clientID, finalTopTenGames)

	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)

	log.Infof("Publishing message to routing key: %s", routingKey)

	err := m.WriterExchange.Publish(routingKey, queryMessage)

	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) AckLastMessage() error {
	err := m.TopTenAccumulatorQueue.AckLastMessages()
	if err != nil {
		return fmt.Errorf("failed to ack last message: %v", err)
	}
	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
