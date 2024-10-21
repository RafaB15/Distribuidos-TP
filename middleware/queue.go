package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	channel     *amqp.Channel
	data        *amqp.Queue
	messages    <-chan amqp.Delivery
	lastMessage *amqp.Delivery
}

func NewQueue(ch *amqp.Channel, name string, autoAck bool) (*Queue, error) {

	err := ch.Qos(
		10,    // prefetch count
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
		channel:     ch,
		data:        &queueData,
		messages:    msgs,
		lastMessage: nil,
	}, nil
}

func (q *Queue) Consume() ([]byte, error) {

	msg, ok := <-q.messages
	if !ok {
		return nil, fmt.Errorf("channel closed")
	}
	q.lastMessage = &msg
	return msg.Body, nil

}

// usar para el rev mapper
func (q *Queue) ConsumeAndCheckEOF(EOFQueue *Queue) ([]byte, bool, error) {

	select {
	case msg, ok := <-q.messages:
		if !ok {
			return nil, false, fmt.Errorf("channel closed")
		}
		q.lastMessage = &msg
		return msg.Body, false, nil
	case <-time.After(2 * time.Second):
		eofMsg, err := EOFQueue.GetIfAvailable()
		if err != nil {
			//timeout = time.Second * 2
			//continue
		}
		return eofMsg.Body, true, nil

	}
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

func (q *Queue) AckLastMessage() error {
	if q.lastMessage == nil {
		return nil
	}
	return q.lastMessage.Ack(false)
}

func (q *Queue) GetIfAvailable() (*amqp.Delivery, error) {
	msg, ok, err := q.channel.Get(q.data.Name, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no message available")
	}
	return &msg, nil
}

func (q *Queue) CloseQueue() error {
	if q.channel != nil {
		return q.channel.Close()
	}
	return nil
}
