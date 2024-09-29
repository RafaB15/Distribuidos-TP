package main

import (
	cp "distribuidos-tp/internal/clientprotocol"
	"distribuidos-tp/internal/mom"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"
	exchangeName  = "game_exchange"
	exchangeType  = "direct"
	routingKey    = "game_key"
)

var log = logging.MustGetLogger("log")

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

	manager, err := mom.NewMiddlewareManager(middlewareURI, 5, 2)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	exchange, err := manager.CreateExchange(exchangeName, exchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
	}
	defer exchange.CloseExchange()

	data, err := cp.ReceiveGameBatch(conn)
	if err != nil {
		log.Errorf("Error receiving game batch:", err)
		return
	}

	lines, err := cp.DeserializeGameBatch(data)
	if err != nil {
		fmt.Println("Error deserializing game batch:", err)
		return
	}

	for _, line := range lines {
		err := exchange.Publish(routingKey, []byte(line))
		if err != nil {
			fmt.Println("Error publishing message:", err)
		}
	}
}
