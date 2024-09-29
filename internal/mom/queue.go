package mom

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	channel *amqp.Channel
	data    *amqp.Queue
}

func NewQueue(ch *amqp.Channel, name string) (*Queue, error) {

	queueData, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &Queue{
		channel: ch,
		data:    &queueData,
	}, nil
}

func (q *Queue) Bind(exchange string, routingKey string, retries int, delaySeconds int) error {
	delay := time.Duration(delaySeconds) * time.Second
	var err error = nil
	for i := 0; i < retries; i++ {
		err = q.channel.QueueBind(
			q.data.Name, // queue name
			routingKey,  // routing key
			exchange,    // exchange
			false,
			nil,
		)
		if err == nil {
			break
		}
		if i == retries-1 {
			return err
		}
		time.Sleep(delay)
	}
	return nil
}

func (q *Queue) Consume(autoAck bool) (<-chan amqp.Delivery, error) {
	msgs, err := q.channel.Consume(
		q.data.Name, // queue
		"",          // consumer
		autoAck,     // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

func (q *Queue) CloseQueue() error {
	if q.channel != nil {
		return q.channel.Close()
	}
	return nil
}
