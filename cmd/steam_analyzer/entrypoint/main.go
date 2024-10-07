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
	middlewareURI      = "amqp://guest:guest@rabbitmq:5672/"
	gameExchangeName   = "game_exchange"
	gameExchangeType   = "direct"
	reviewExchangeType = "fanout"
	gameRoutingKey     = "game_key"
	reviewRoutingKey   = "review_key"
	gameQueueName      = "game_queue"
	reviewQueueName    = "reviews_queue"
	rawReviewQueueName = "raw_reviews_queue"
	reviewExchangeName = "raw_reviews_exchange"
)

const GameFile = 1
const ReviewFile = 0

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	gameQueue, err := manager.CreateQueue(gameQueueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	gameExchange, err := manager.CreateExchange(gameExchangeName, gameExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	err = gameQueue.Bind(gameExchange.Name, gameRoutingKey)

	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return
	}

	defer gameExchange.CloseExchange()

	reviewQueue, err := manager.CreateQueue(reviewQueueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	rawReviewQueue, err := manager.CreateQueue(rawReviewQueueName)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
	}

	reviewExchange, err := manager.CreateExchange(reviewExchangeName, reviewExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	err = reviewQueue.Bind(reviewExchange.Name, reviewRoutingKey)

	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return
	}

	err = rawReviewQueue.Bind(reviewExchange.Name, reviewRoutingKey)

	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return
	}

	defer reviewExchange.CloseExchange()

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

		go handleConnection(conn, gameExchange, reviewExchange)
	}
}

func handleConnection(conn net.Conn, gameExchange *mom.Exchange, reviewExchange *mom.Exchange) {
	defer conn.Close()

	for {
		data, fileOrigin, eofFlag, err := cp.ReceiveBatch(conn)
		if err != nil {
			log.Errorf("Error receiving game batch:", err)
			return
		}

		// Se deberían mandar varios por paquete
		batch := sp.SerializeBatchMsg(data)
		if fileOrigin == GameFile {
			err = gameExchange.Publish(gameRoutingKey, batch)
		} else {
			err = reviewExchange.Publish(reviewRoutingKey, batch)
		}
		if err != nil {
			fmt.Println("Error publishing message:", err)
		}

		if eofFlag {
			if fileOrigin == GameFile {
				err = gameExchange.Publish(gameRoutingKey, sp.SerializeMsgEndOfFile())
				log.Infof("End of file message published for games")
			} else {
				err = reviewExchange.Publish(reviewRoutingKey, sp.SerializeMsgEndOfFile())
				log.Infof("End of file message published for reviews")
			}
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}
		}
	}
}
