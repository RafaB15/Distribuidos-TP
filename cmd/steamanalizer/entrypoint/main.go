package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	listener, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server listening on port 3000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}

	fmt.Println("Received message:", message)

	var amqpConn *amqp.Connection
	for i := 0; i < 10; i++ {
		amqpConn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
		if err == nil {
			break
		}
		fmt.Println("Retrying connection to RabbitMQ...")
		time.Sleep(5 * time.Second)
	}
	defer amqpConn.Close()

	ch, err := amqpConn.Channel()
	if err != nil {
		fmt.Println("Error creating channel:", err)
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"first_exchange", // name. Nombre del exchange
		"direct",         // type. Tipo del exchange. Manda mensajes a las colas que matchee la routing key.
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		fmt.Println("Error declaring exchange:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"first_exchange", // exchange
		"example",        // routing key. Se ignora para el fanout exchange.
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		fmt.Println("Error publishing message:", err)
		return
	}

	log.Printf(" [x] Sent %s", message)
}
