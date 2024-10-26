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
	TopTenAccumulatorExchange *mom.Exchange // Este exchange es para el envío al siguiente nodo
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

func (m *Middleware) ReceiveYearAvgPtf() (int, []*df.GameYearAndAvgPtf, bool, error) {

	rawMsg, err := m.YearAvgPtfQueue.Consume()
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
		gamesYearsAvgPtfs, err := sp.DeserializeMsgGameYearAndAvgPtf(message.Body)

		if err != nil {
			return message.ClientID, nil, false, err
		}

		return message.ClientID, gamesYearsAvgPtfs, false, nil
	default:
		return message.ClientID, nil, false, nil
	}

}

func (m *Middleware) SendFilteredYearAvgPtf(clientID int, gamesYearsAvgPtfs []*df.GameYearAndAvgPtf) error {
	data := sp.SerializeMsgGameYearAndAvgPtf(clientID, gamesYearsAvgPtfs)

	fmt.Printf("About to publish to top ten accumulator exchange\n")
	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, data)
	if err != nil {
		return err
	}
	fmt.Printf("Published to top ten accumulator exchange\n")

	return nil
}

func (m *Middleware) SendEof(clientID int) error {

	err := m.TopTenAccumulatorExchange.Publish(TopTenAccumulatorRoutingKey, sp.SerializeMsgEndOfFile(clientID))
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) Close() error {
	return m.Manager.CloseConnection()
}
