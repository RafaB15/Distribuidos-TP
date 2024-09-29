package mom

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareManager struct {
	conn *amqp.Connection
}

func NewMiddlewareManager(middlewareURI string, retries int, delaySeconds int) (*MiddlewareManager, error) {
	var conn *amqp.Connection
	var err error
	delay := time.Duration(delaySeconds) * time.Second

	for i := 0; i < retries; i++ {
		fmt.Println("Doing my best")
		conn, err = amqp.Dial(middlewareURI)
		if err == nil {
			break
		}
		time.Sleep(delay)
	}

	if err != nil {
		return nil, err
	}

	manager := &MiddlewareManager{
		conn,
	}

	return manager, nil
}

func (m *MiddlewareManager) CreateQueue(name string, exchange string, routingKey string) (*Queue, error) {
	ch, err := m.conn.Channel()
	if err != nil {
		println("Primer")
		return nil, err
	}

	queue, err := NewQueue(ch, name)
	if err != nil {
		println("Segundo")
		return nil, err
	}

	err = queue.Bind(exchange, routingKey, 5, 2)
	if err != nil {
		println("Tercero")
		queue.CloseQueue()
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
