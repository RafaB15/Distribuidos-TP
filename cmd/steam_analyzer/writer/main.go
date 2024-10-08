package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	u "distribuidos-tp/internal/utils"
	"os"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
	WriterQueueName    = "writer_queue"
)

var log = logging.MustGetLogger("log")

func main() {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		log.Errorf("Failed to create middleware manager: %v", err)
		return
	}
	defer manager.CloseConnection()

	writerQueue, err := manager.CreateBoundQueue(WriterQueueName, WriterExchangeName, WriterExchangeType, WriterRoutingKey)
	if err != nil {
		log.Errorf("Failed to create queue: %v", err)
		return
	}

	msgs, err := writerQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			msgType, err := sp.DeserializeMessageType(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize message type: %v", err)
				return
			}
			data := d.Body[1:]
			switch msgType {
			case sp.MsgQueryResolved:

				query, err := sp.DeserializeQueryResolvedType(data)
				if err != nil {
					log.Errorf("Failed to deserialize query resolved message: %v", err)
					return
				}
				log.Infof("Received query resolved message: %v", query)

				switch query {
				case sp.MsgOsResolvedQuery:
					log.Info("Received query Os resolved message")
					err := handleOsResolvedQuery(data)
					if err != nil {
						log.Errorf("Failed to handle os resolved query: %v", err)
						return
					}
				case sp.MsgActionPositiveReviewsQuery:
					log.Info("Received query Action positive reviews resolved message")
					err := handleGenrePositiveReviewsQuery(data[1:], "action_positive_reviews_query.txt")
					if err != nil {
						log.Errorf("Failed to handle action positive reviews resolved query: %v", err)
						return
					}
				case sp.MsgTopTenDecadeAvgPtfQuery:
					log.Info("Received query Top ten decade avg ptf resolved message")
					err := handleTopTenDecadeAvgPtfQuery(data[1:])
					if err != nil {
						log.Errorf("Failed to handle top ten decade avg ptf resolved query: %v", err)
						return
					}
				case sp.MsgIndiePositiveJoinedReviewsQuery:
					log.Info("Received query Indie positive joined reviews resolved message")
					err := handleGenrePositiveReviewsQuery(data[1:], "top_positive_indie_reviews_query.txt")
					if err != nil {
						log.Errorf("Failed to handle top positive indie reviews resolved query: %v", err)
						return
					}
				case sp.MsgActionNegativeReviewsQuery:
					log.Info("Received query Action negative reviews resolved message")
					err := handleGenrePositiveReviewsQuery(data[1:], "percentile_negative_action_reviews_query.txt")
					if err != nil {
						log.Errorf("Failed to handle percentile negative action reviews resolved query: %v", err)
						return
					}
				}

			default:
				log.Errorf("Invalid message type: %v", msgType)
			}
		}
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func handleOsResolvedQuery(data []byte) error {

	file, err := os.OpenFile("os_query.txt", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()

	err = u.WriteAllToFile(file, data)

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to os_query file")
	return nil

}

func handleGenrePositiveReviewsQuery(data []byte, name_file string) error {

	file, err := os.OpenFile(name_file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()

	err = u.WriteAllToFile(file, data)

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to action_positive_reviews_query file")
	return nil

}

func handleTopTenDecadeAvgPtfQuery(data []byte) error {

	file, err := os.OpenFile("top_ten_decade_avg_ptf_query.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()
	err = u.WriteAllToFile(file, data)

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to top_ten_decade_avg_ptf_query file")
	return nil

}
