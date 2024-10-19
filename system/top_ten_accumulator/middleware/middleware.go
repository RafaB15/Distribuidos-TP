package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	mom "distribuidos-tp/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	TopTenAccumulatorExchangeName = "top_ten_accumulator_exchange"
	TopTenAccumulatorExchangeType = "direct"
	TopTenAccumulatorRoutingKey   = "top_ten_accumulator_key"
	TopTenAccumulatorQueueName    = "top_ten_accumulator_queue"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"

	DecadeFiltersAmountEnvironmentVariableName = "DECADE_FILTERS_AMOUNT"
)

type Middleware struct {
	Manager                *mom.MiddlewareManager
	TopTenAccumulatorQueue *mom.Queue
	WriterExchange         *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	topTenAccumulatorQueue, err := manager.CreateBoundQueue(TopTenAccumulatorQueueName, TopTenAccumulatorExchangeName, TopTenAccumulatorExchangeType, TopTenAccumulatorRoutingKey, true)
	if err != nil {
		return nil, err
	}

	writerExchange, err := manager.CreateExchange(WriterExchangeName, WriterExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                manager,
		TopTenAccumulatorQueue: topTenAccumulatorQueue,
		WriterExchange:         writerExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() ([]*df.GameYearAndAvgPtf, bool, error) {
	msg, err := m.TopTenAccumulatorQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil

	case sp.MsgFilteredYearAndAvgPtfInformation:
		decadeGames, err := sp.DeserializeMsgGameYearAndAvgPtf(msg)
		if err != nil {
			return nil, false, err
		}

		return decadeGames, false, nil

	default:
		return nil, false, nil
	}
}

func (m *Middleware) SendMsg(finalTopTenGames []*df.GameYearAndAvgPtf) error {
	srzGames := df.SerializeTopTenAvgPlaytimeForever(finalTopTenGames)
	bytes := sp.SerializeTopTenDecadeAvgPtfQueryMsg(srzGames)
	err := m.WriterExchange.Publish(WriterRoutingKey, bytes)

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
