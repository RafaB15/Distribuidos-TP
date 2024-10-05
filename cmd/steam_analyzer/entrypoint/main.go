package main

import (
	cp "distribuidos-tp/internal/client_protocol"
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"
	exchangeName  = "game_exchange"
	exchangeType  = "direct"
	routingKey    = "game_key"
	queueName     = "game_queue"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	queue, err := manager.CreateQueue(queueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	exchange, err := manager.CreateExchange(exchangeName, exchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	err = queue.Bind(exchange.Name, routingKey)

	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return
	}

	defer exchange.CloseExchange()

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

		go handleConnection(conn, exchange)
	}
}

func handleConnection(conn net.Conn, exchange *mom.Exchange) {
	defer conn.Close()

	for {
		data, _, eofFlag, err := cp.ReceiveBatch(conn)
		if err != nil {
			log.Errorf("Error receiving game batch:", err)
			return
		}

		lines, err := cp.DeserializeBatch(data)
		if err != nil {
			fmt.Println("Error deserializing game batch:", err)
			return
		}

		for _, line := range lines {
			log.Infof("About to publish message: %s", line)
			batch := sp.SerializeBatchMsg(line)
			err := exchange.Publish(routingKey, batch)
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}
		}

		if eofFlag {
			err := exchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
			log.Infof("End of file message published")
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}
		}
	}
}
