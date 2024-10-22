package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	YearAvgPtfExchangeName = "year_avg_ptf_exchange"
	YearAvgPtfExchangeType = "direct"
	YearAvgPtfRoutingKey   = "year_avg_ptf_key"
	YearAvgPtfQueueName    = "year_avg_ptf_queue"

	TopTenAccumulatorExchangeName = "top_ten_accumulator_exchange"
	TopTenAccumulatorExchangeType = "direct"
	TopTenAccumulatorRoutingKey   = "top_ten_accumulator_key"
)

type Middleware struct {
	Manager                   *mom.MiddlewareManager
	YearAvgPtfQueue           *mom.Queue    // Esta cola es para recibir del nodo anterior
	TopTenAccumulatorExchange *mom.Exchange // Este exchange es para el envio al siguiente nodo
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	yearAvgPtfQueue, err := manager.CreateBoundQueue(YearAvgPtfQueueName, YearAvgPtfExchangeName, YearAvgPtfExchangeType, YearAvgPtfRoutingKey, true)
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
	}, nil
}

func (m *Middleware) ReceiveYearAvgPtf() ([]*df.GameYearAndAvgPtf, bool, error) {

	rawMsg, err := m.YearAvgPtfQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return nil, false, err
	}

	switch message.Type {

	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgGameYearAndAvgPtfInformation:
		gamesYearsAvgPtfs, err := sp.DeserializeMsgGameYearAndAvgPtfV2(message.Body)

		if err != nil {
			return nil, false, err
		}

		return gamesYearsAvgPtfs, false, nil
	default:
		return nil, false, nil
	}

}

func (m *Middleware) SendFilteredYearAvgPtf(gamesYearsAvgPtfs []*df.GameYearAndAvgPtf) error {
	data := sp.SerializeMsgFilteredGameYearAndAvgPtf(gamesYearsAvgPtfs)

	fmt.Printf("About to publish to top ten accumulator exchange\n")
	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, data)
	fmt.Printf("Published to top ten accumulator exchange\n")
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) SendEof() error {
	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	return nil
}
