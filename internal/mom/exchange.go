package mom

import amqp "github.com/rabbitmq/amqp091-go"

type Exchange struct {
	channel *amqp.Channel
	Name    string
	kind    string
}

func NewExchange(ch *amqp.Channel, name string, kind string) (*Exchange, error) {
	err := ch.ExchangeDeclare(
		name,  // name
		kind,  // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &Exchange{
		channel: ch,
		Name:    name,
		kind:    kind,
	}, nil
}

func (e *Exchange) Publish(routingKey string, body []byte) error {
	err := e.channel.Publish(
		e.Name,     // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		return err
	}
	return nil
}

func (e *Exchange) CloseExchange() error {
	if e.channel != nil {
		return e.channel.Close()
	}
	return nil
}
