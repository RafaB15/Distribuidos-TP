package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	mom "distribuidos-tp/middleware"
	"fmt"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	OSGamesExchangeName     = "os_games_exchange"
	OSGamesExchangeType     = "direct"
	OSGamesRoutingKeyPrefix = "os_games_key_"
	OSGamesQueueNamePrefix  = "os_games_queue_"

	OSAccumulatorExchangeName = "os_accumulator_exchange"
	OSAccumulatorRoutingKey   = "os_accumulator_key"
	OSAccumulatorExchangeType = "direct"
)

type Middleware struct {
	Manager               *mom.MiddlewareManager
	OSGamesQueue          *mom.Queue
	OSAccumulatorExchange *mom.Exchange
}

func NewMiddleware(id int) (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	osGamesQueueName := fmt.Sprintf("%s%d", OSGamesQueueNamePrefix, id)
	osGamesRoutingKey := fmt.Sprintf("%s%d", OSGamesRoutingKeyPrefix, id)

	osGamesQueue, err := manager.CreateBoundQueue(osGamesQueueName, OSGamesExchangeName, OSGamesExchangeType, osGamesRoutingKey, true)
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

func (m *Middleware) SendMetrics(clientID int, gameMetrics *oa.GameOSMetrics) error {
	data := sp.SerializeGameOSMetrics(clientID, gameMetrics)

	err := m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, data)

	if err != nil {
		return err
	}

	return nil
}

// Returns a slice of GameOS structs, a boolean indicating if the end of the file was reached and an error
func (m *Middleware) ReceiveGameOS() (int, []*oa.GameOS, bool, error) {
	rawMsg, err := m.OSGamesQueue.Consume()
	if err != nil {
		return 0, nil, false, err
	}

	message, err := sp.DeserializeMessage(rawMsg)
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to deserialize message: %v", err)
	}

	switch message.Type {

	case sp.MsgEndOfFile:
		return message.ClientID, nil, true, nil
	case sp.MsgGameOSInformation:
		gamesOs, err := sp.DeserializeMsgGameOSInformationV2(message.Body)

		if err != nil {
			return message.ClientID, nil, false, err
		}

		return message.ClientID, gamesOs, false, nil
	default:
		return message.ClientID, nil, false, nil
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

func (m *Middleware) SendEof(clientID int) error {

	err := m.OSAccumulatorExchange.Publish(OSAccumulatorRoutingKey, sp.SerializeMsgEndOfFileV2(clientID))
	if err != nil {
		return err
	}

	return nil
}
