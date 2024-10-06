package mom

import (
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

func (m *MiddlewareManager) CreateQueue(name string) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {

		return nil, err
	}

	queue, err := NewQueue(ch, name)
	if err != nil {

		return nil, err
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
	if m.conn != nil {
		return m.conn.Close()
	}
	return nil
}