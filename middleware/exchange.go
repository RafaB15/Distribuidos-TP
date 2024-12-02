package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Exchange struct {
	channel  *amqp.Channel
	Name     string
	kind     string
	confirms chan amqp.Confirmation
}

// NewExchange creates a new exchange and enables publisher confirms
func NewExchange(ch *amqp.Channel, name string, kind string) (*Exchange, error) {
	// Declare the exchange
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

	// Enable publisher confirms
	err = ch.Confirm(false)
	if err != nil {
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	// Create and return the Exchange struct
	ex := &Exchange{
		channel:  ch,
		Name:     name,
		kind:     kind,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}
	return ex, nil
}

// Publish publishes a message and waits for confirmation
func (e *Exchange) Publish(routingKey string, body []byte) error {
	// Publish the message
	err := e.channel.Publish(
		e.Name,     // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation
	select {
	case confirm := <-e.confirms:
		if confirm.Ack {
			log.Printf("Message confirmed: deliveryTag=%d", confirm.DeliveryTag)
			return nil
		}
		log.Printf("Message nack'd: deliveryTag=%d", confirm.DeliveryTag)
		return fmt.Errorf("message was nack'd by broker")
	case <-e.channel.NotifyClose(make(chan *amqp.Error)):
		return fmt.Errorf("channel closed before confirmation received")
	}
}

func (e *Exchange) PublishWithPriority(routingKey string, body []byte, priority uint8) error {
	err := e.channel.Publish(
		e.Name,     // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
			Priority:    priority,
		})
	if err != nil {
		return err
	}

	// Esperamos la confirmación
	select {
	case confirm := <-e.confirms:
		if confirm.Ack {
			log.Printf("Message confirmed: deliveryTag=%d", confirm.DeliveryTag)
			return nil
		}
		log.Printf("Message nack'd: deliveryTag=%d", confirm.DeliveryTag)
		return fmt.Errorf("message was nack'd by broker")
	case <-e.channel.NotifyClose(make(chan *amqp.Error)):
		return fmt.Errorf("channel closed before confirmation received")
	}
}

func (e *Exchange) CloseExchange() error {
	if e.channel != nil {
		return e.channel.Close()
	}
	return nil
}
