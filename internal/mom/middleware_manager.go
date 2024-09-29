package mom

import amqp "github.com/rabbitmq/amqp091-go"

type MiddlewareManager struct {
	conn *amqp.Connection
}

func NewMiddlewareManager(middlewareURI string) (*MiddlewareManager, error) {
	conn, err := amqp.Dial(middlewareURI)
	if err != nil {
		return nil, err
	}

	manager := &MiddlewareManager{
		conn,
	}

	return manager, nil
}

func (m *MiddlewareManager) createQueue(name string, exchange string, routingKey string) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		return nil, err
	}

	return NewQueue(ch, name, exchange, routingKey)
}
