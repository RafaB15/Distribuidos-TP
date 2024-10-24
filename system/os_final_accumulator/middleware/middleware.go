package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
	OSAccumulatorQueueName    = "os_accumulator_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
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

func (m *Middleware) ReceiveGamesOSMetrics() (int, *oa.GameOSMetrics, bool, error) {
	rawMsg, err := m.OSAccumulatorQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)

	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgAccumulatedGameOSInformation:
		gameMetrics, err := sp.DeserializeMsgAccumulatedGameOSInformationV2(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}

		return message.ClientID, gameMetrics, false, nil

	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	default:
		return message.ClientID, nil, false, fmt.Errorf("received unexpected message type: %v", message.Type)
	}

}

func (m *Middleware) SendFinalMetrics(gameMetrics *oa.GameOSMetrics) error {
	data := oa.SerializeGameOSMetrics(gameMetrics)
	msg := sp.SerializeOsResolvedQueryMsg(data)

	err := m.WriterExchange.Publish(WriterRoutingKey, msg)
	if err != nil {
		return err
	}

	err = m.WriterExchange.Publish(WriterRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}
	log.Infof("sent EOF to writer")

	return nil
}
