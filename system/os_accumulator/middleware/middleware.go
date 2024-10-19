package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	mom "distribuidos-tp/middleware"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSGamesExchangeName = "os_games_exchange"
	OSGamesRoutingKey   = "os_games_key"
	OSGamesExchangeType = "direct"
	OSGamesQueueName    = "os_games_queue"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
)

type Middleware struct {
	Manager               *mom.MiddlewareManager
	OSGamesQueue          *mom.Queue
	OSAccumulatorExchange *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	osGamesQueue, err := manager.CreateBoundQueue(OSGamesQueueName, OSGamesExchangeName, OSGamesExchangeType, OSGamesRoutingKey, true)
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
	}, nil
}

func (m *Middleware) SendMetrics(gameMetrics *oa.GameOSMetrics) error {
	data := oa.SerializeGameOSMetrics(gameMetrics)
	msg, err := sp.SerializeMsgAccumulatedGameOSInfo(data)

	if err != nil {
		return err
	}

	err = m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, msg)

	if err != nil {
		return err
	}

	return nil
}

// Returns a slice of GameOS structs, a boolean indicating if the end of the file was reached and an error
func (m *Middleware) ReceiveGameOS() ([]*oa.GameOS, bool, error) {
	msg, err := m.OSGamesQueue.Consume()
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
	case sp.MsgGameOSInformation:
		gamesOs, err := sp.DeserializeMsgGameOSInformation(msg)

		if err != nil {
			return nil, false, err
		}

		return gamesOs, false, nil
	default:
		return nil, false, nil
	}
}

// Shutdown method to close the MiddlewareManager and related resources
func (m *Middleware) Shutdown() error {

	// Cerrar colas y exchanges
	if err := m.Manager.CloseConnection(); err != nil {
		return err
	}

	return nil
}
