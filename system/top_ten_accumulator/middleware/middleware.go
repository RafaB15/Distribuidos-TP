package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
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

	writerExchange, err := manager.CreateExchange(QueryResultsExchangeName, QueryExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:                manager,
		TopTenAccumulatorQueue: topTenAccumulatorQueue,
		WriterExchange:         writerExchange,
	}, nil
}

func (m *Middleware) ReceiveMsg() (int, []*df.GameYearAndAvgPtf, bool, error) {
	rawMsg, err := m.TopTenAccumulatorQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, err
	}

	switch message.Type {
	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil

	case sp.MsgGameYearAndAvgPtfInformation:
		decadeGames, err := sp.DeserializeMsgGameYearAndAvgPtf(message.Body)
		if err != nil {
			return message.ClientID, nil, false, err
		}

		return message.ClientID, decadeGames, false, nil

	default:
		return message.ClientID, nil, false, nil
	}
}

func (m *Middleware) SendMsg(clientID int, finalTopTenGames []*df.GameYearAndAvgPtf) error {

	queryMessage := sp.SerializeMsgTopTenResolvedQuery(clientID, finalTopTenGames)

	routingKey := fmt.Sprintf("%s%d", QueryRoutingKeyPrefix, clientID)
	// // debug

	// queryResponseMessage, _ := sp.DeserializeQuery(queryMessage)
	// log.Infof("Query Type %d", queryResponseMessage.Type)
	// log.Infof("For client %d", queryResponseMessage.ClientID)

	// // debug
	log.Infof("Publishing message to routing key: %s", routingKey)

	err := m.WriterExchange.Publish(routingKey, queryMessage)

	if err != nil {
		return err
	}

	return nil
}
