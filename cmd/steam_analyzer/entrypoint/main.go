package main

import (
	"context"
	cp "distribuidos-tp/internal/client_protocol"
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

const (
	MiddlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawGamesExchangeName = "raw_games_exchange"
	RawGamesRoutingKey   = "raw_games_key"
	RawGamesExchangeType = "direct"

	RawReviewsExchangeName = "raw_reviews_exchange"
	RawReviewsExchangeType = "fanout"

	RawReviewsEofExchangeName = "raw_reviews_eof_exchange"
	RawReviewsEofExchangeType = "fanout"
)

const GameFile = 1
const ReviewFile = 0

var log = logging.MustGetLogger("log")
var wg sync.WaitGroup // WaitGroup para sincronizar la finalización

func main() {
	// Create a context that will be canceled on SIGINT or SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	manager, err := mom.NewMiddlewareManager(MiddlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	rawGamesExchange, err := manager.CreateExchange(RawGamesExchangeName, RawGamesExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	defer rawGamesExchange.CloseExchange()

	rawReviewsExchange, err := manager.CreateExchange(RawReviewsExchangeName, RawReviewsExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	rawReviewsEofExchange, err := manager.CreateExchange(RawReviewsEofExchangeName, RawReviewsEofExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	defer rawReviewsExchange.CloseExchange()

	listener, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server listening on port 3000")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					// Context canceled, stop accepting new connections
					return
				default:
					fmt.Println("Error accepting connection:", err)
					continue
				}
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				handleConnection(conn, rawGamesExchange, rawReviewsExchange, rawReviewsEofExchange)
			}()
		}
	}()

	go func() {
		sig := <-sigs
		log.Infof("Received signal: %v. Waiting for tasks to complete...", sig)
		wg.Wait() // Esperar a que todas las tareas en el WaitGroup terminen
		log.Info("All tasks completed. Shutting down.")
		done <- true
	}()

	<-done
}

func handleConnection(conn net.Conn, rawGamesExchange *mom.Exchange, rawReviewsExchange *mom.Exchange, rawReviewsEofExchange *mom.Exchange) {
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
			err = rawGamesExchange.Publish(RawGamesRoutingKey, batch)
		} else {
			err = rawReviewsExchange.PublishWithoutKey(batch)
		}
		if err != nil {
			fmt.Println("Error publishing message:", err)
		}

		if eofFlag {
			if fileOrigin == GameFile {
				err = rawGamesExchange.Publish(RawGamesRoutingKey, sp.SerializeMsgEndOfFile())
				log.Infof("End of file message published for games")
			} else {
				err = rawReviewsEofExchange.PublishWithoutKey(sp.SerializeMsgEndOfFile())
				log.Infof("End of file message published for reviews")
			}
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}
		}
	}
}
