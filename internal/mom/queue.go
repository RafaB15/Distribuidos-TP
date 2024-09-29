package mom

import amqp "github.com/rabbitmq/amqp091-go"

type Queue struct {
	channel *amqp.Channel
	data    *amqp.Queue
}

func NewQueue(ch *amqp.Channel, name string, exchange string, routingKey string) (*Queue, error) {

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

	err = ch.QueueBind(
		queueData.Name, // queue name
		routingKey,     // routing key
		exchange,       // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Queue{
		channel: ch,
		data:    &queueData,
	}, nil
}

func (q *Queue) Consume(consumerName string, autoAck bool) (<-chan amqp.Delivery, error) {
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
