package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
	OSAccumulatorQueueName    = "os_accumulator_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	NumPreviousNodes = "NUM_PREVIOUS_OS_ACCUMULATORS"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	OSAccumulatorQueue *mom.Queue
	WriterExchange     *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	osAccumulatorQueue, err := manager.CreateBoundQueue(OSAccumulatorQueueName, OSAccumulatorExchangeName, OSAccumulatorExchangeType, OSAccumulatorRoutingKey, true)
	if err != nil {
		return nil, err
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:            manager,
		OSAccumulatorQueue: osAccumulatorQueue,
		WriterExchange:     writerExchange,
	}, nil
}

func (m *Middleware) ReceiveGamesOSMetrics() (*oa.GameOSMetrics, error) {
	msg, err := m.OSAccumulatorQueue.Consume()
	if err != nil {
		return nil, err
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, err
	}

	if messageType != sp.MsgAccumulatedGameOSInformation {
		return nil, fmt.Errorf("Received message of type %d, expected %d", messageType, sp.MsgAccumulatedGameOSInformation)
	}

	gameMetrics, err := sp.DeserializeMsgAccumulatedGameOSInformation(msg)
	if err != nil {
		return nil, err
	}

	return gameMetrics, nil
}

func (m *Middleware) SendFinalMetrics(gameMetrics *oa.GameOSMetrics) error {
	data := oa.SerializeGameOSMetrics(gameMetrics)
	msg := sp.SerializeOsResolvedQueryMsg(data)

	err := m.WriterExchange.Publish(WriterRoutingKey, msg)
	if err != nil {
		return err
	}

	return nil
}
