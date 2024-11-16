package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	channel            *amqp.Channel
	data               *amqp.Queue
	messages           <-chan amqp.Delivery
	lastUnackedMessage *amqp.Delivery
}

func NewQueue(ch *amqp.Channel, name string, autoAck bool) (*Queue, error) {

	err := ch.Qos(
		100,   // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

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

	msgs, err := ch.Consume(
		queueData.Name, // queue
		"",             // consumer
		autoAck,        // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)

	if err != nil {
		return nil, err
	}

	return &Queue{
		channel:            ch,
		data:               &queueData,
		messages:           msgs,
		lastUnackedMessage: nil,
	}, nil
}

func (q *Queue) Consume() ([]byte, error) {

	msg, ok := <-q.messages
	if !ok {
		return nil, fmt.Errorf("channel closed")
	}
	q.lastUnackedMessage = &msg
	return msg.Body, nil
}

func (q *Queue) Bind(exchange string, routingKey string) error {

	err := q.channel.QueueBind(
		q.data.Name, // queue name
		routingKey,  // routing key
		exchange,    // exchange
		false,
		nil,
	)

	return err
}

func (q *Queue) AckLastMessages() error {
	if q.lastUnackedMessage != nil {
		err := q.lastUnackedMessage.Ack(true)
		if err != nil {
			return err
		}
		q.lastUnackedMessage = nil
	}
	return nil
}

func (q *Queue) CloseQueue() error {
	if q.channel != nil {
		return q.channel.Close()
	}
	return nil
}
