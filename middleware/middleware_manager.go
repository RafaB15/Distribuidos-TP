package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareManager struct {
	conn *amqp.Connection
}

func NewMiddlewareManager(middlewareURI string) (*MiddlewareManager, error) {
	var conn *amqp.Connection
	var err error
	conn, err = amqp.Dial(middlewareURI)

	if err != nil {
		return nil, err
	}

	manager := &MiddlewareManager{
		conn,
	}

	return manager, nil
}

func (m *MiddlewareManager) CreateBoundQueue(queueName string, exchangeName string, exchangeKind string, routingKey string, autoAck bool) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		return nil, err
	}

	queue, err := NewQueue(ch, queueName, autoAck)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	_, err = NewExchange(ch, exchangeName, exchangeKind)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange to bind the queue: %w", err)
	}

	err = queue.Bind(exchangeName, routingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	return queue, nil
}

func (m *MiddlewareManager) CreateBoundQueueMultipleRoutingKeys(queueName string, exchangeName string, exchangeKind string, routingKey []string, autoAck bool) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		return nil, err
	}

	queue, err := NewQueue(ch, queueName, autoAck)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	_, err = NewExchange(ch, exchangeName, exchangeKind)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange to bind the queue: %w", err)
	}

	for _, key := range routingKey {
		err = queue.Bind(exchangeName, key)
		if err != nil {
			return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
		}
	}

	return queue, nil
}

func (m *MiddlewareManager) CreateBoundQueueWithPriority(queueName string, exchangeName string, exchangeKind string, routingKey string, autoAck bool, maxPriority int) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		return nil, err
	}

	queue, err := NewPriorityQueue(ch, queueName, autoAck, maxPriority)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	_, err = NewExchange(ch, exchangeName, exchangeKind)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange to bind the queue: %w", err)
	}

	err = queue.Bind(exchangeName, routingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	return queue, nil
}

func (m *MiddlewareManager) CreateExchange(name string, kind string) (*Exchange, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		return nil, err
	}

	return NewExchange(ch, name, kind)
}

func (m *MiddlewareManager) CloseConnection() error {
	fmt.Print("Closing connection to rabbit")
	if m.conn != nil {
		return m.conn.Close()
	}
	return nil
}
