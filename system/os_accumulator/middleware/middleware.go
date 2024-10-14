package middleware

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
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
	OSGamesQueue          *mom.Queue
	OSAccumulatorExchange *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	defer manager.CloseConnection()

	osGamesQueue, err := manager.CreateBoundQueue(OSGamesQueueName, OSGamesExchangeName, OSGamesExchangeType, OSGamesRoutingKey)
	if err != nil {
		return nil, err
	}

	osAccumulatorExchange, err := manager.CreateExchange(OSAccumulatorExchangeName, OSAccumulatorExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
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

func (m *Middleware) ReceiveMetrics() ([]*oa.GameOS, bool, error) {
	msg, err := m.OSGamesQueue.ConsumeOne() // Luego la implemento bien
	if err != nil {
		return nil, err
	}

	gamesOS, err := sp.DeserializeMsgGameOSInformation(msg.Body)
	if err != nil {
		return nil, err
	}

	return gamesOS, false, nil // El bool debería ser por el eof
}
