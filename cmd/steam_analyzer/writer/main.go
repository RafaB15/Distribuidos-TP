package main

import (
	"distribuidos-tp/internal/mom"
	sp "distribuidos-tp/internal/system_protocol"
	oa "distribuidos-tp/internal/system_protocol/accumulator/os_accumulator"
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	jr "distribuidos-tp/internal/system_protocol/joiner"
	u "distribuidos-tp/internal/utils"
	"encoding/binary"
	"io"
	"os"

	"github.com/op/go-logging"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	WriterExchangeName = "writer_exchange"
	WriterRoutingKey   = "writer_key"
	WriterExchangeType = "direct"
	WriterQueueName    = "writer_queue"

	QueryResponseExchangeName = "query_response_exchange"
	QueryResponseExchangeType = "direct"
	QueryResponseRoutingKey   = "query_response_key"

	ActionNegativeReviewsJoinersAmountEnvironmentVariableName = "ACTION_NEGATIVE_REVIEWS_JOINERS_AMOUNT"
	ActionPositiveReviewJoinersAmountEnvironmentVariableName  = "ACTION_POSITIVE_REVIEWS_JOINERS_AMOUNT"

	Query1OSFileName                       = "os_query.txt"
	Query2TopTenDecadeFileName             = "top_ten_decade_avg_ptf_query.txt"
	Query3TopPositiveIndiesFileName        = "top_positive_indie_reviews_query.txt"
	Query4ActionPositiveFileName           = "action_positive_reviews_query.txt"
	Query5ActionNegativePercentileFileName = "percentile_negative_action_reviews_query.txt"
)

var log = logging.MustGetLogger("log")

func main() {

	actionNegativeReviewsJoinersAmount, err := u.GetEnvInt(ActionNegativeReviewsJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	actionPositiveReviewsJoinersAmount, err := u.GetEnvInt(ActionPositiveReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

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

	queryResponseExchange, err := manager.CreateExchange(QueryResponseExchangeName, QueryResponseExchangeType)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return
	}

	msgs, err := writerQueue.Consume(true)
	if err != nil {
		log.Errorf("Failed to consume messages: %v", err)
	}

	forever := make(chan bool)

	go func() {
		remmainingEOFs := actionNegativeReviewsJoinersAmount + actionPositiveReviewsJoinersAmount + 3
	loop:
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
					err := handleOsResolvedQuery(data[1:])
					if err != nil {
						log.Errorf("Failed to handle os resolved query: %v", err)
						return
					}
					sendQueryResult(queryResponseExchange, Query1OSFileName, 1)
				case sp.MsgActionPositiveReviewsQuery:
					log.Info("Received query Action positive reviews resolved message")
					err := handleActionEnfglish5kReviewsQuery(data[1:], "action_positive_reviews_query.txt")
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
					sendQueryResult(queryResponseExchange, Query2TopTenDecadeFileName, 2)
				case sp.MsgIndiePositiveJoinedReviewsQuery:
					log.Info("Received query Indie positive joined reviews resolved message")
					err := handleIndieTopPositiveReviewsQuery(data[1:], "top_positive_indie_reviews_query.txt")
					if err != nil {
						log.Errorf("Failed to handle top positive indie reviews resolved query: %v", err)
						return
					}
					sendQueryResult(queryResponseExchange, Query3TopPositiveIndiesFileName, 3)
				case sp.MsgActionNegativeReviewsQuery:
					log.Info("Received query Action negative reviews resolved message")
					err := handleActionNegativeReviewAbovePercentileQuery(data[1:], "percentile_negative_action_reviews_query.txt")
					if err != nil {
						log.Errorf("Failed to handle percentile negative action reviews resolved query: %v", err)
						return
					}
				}
			case sp.MsgEndOfFile:
				log.Info("End Of File received")
				remmainingEOFs--
				if remmainingEOFs > 0 {
					continue
				}
				log.Info("All EOFs received")
				sendQueryResult(queryResponseExchange, Query4ActionPositiveFileName, 4)
				sendQueryResult(queryResponseExchange, Query5ActionNegativePercentileFileName, 5)
				break loop

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

	gameOsMetric, err := oa.DeserializeGameOSMetrics(data)

	if err != nil {
		log.Errorf("failed to deserialize game os metrics: %v", err)
		return err
	}

	err = u.WriteAllToFile(file, []byte(oa.GetStrRepresentation(gameOsMetric)))

	if err != nil {
		log.Errorf("failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to os_query file")
	return nil

}

func handleTopTenDecadeAvgPtfQuery(data []byte) error {

	file, err := os.OpenFile("top_ten_decade_avg_ptf_query.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()

	resultList, err := df.DeserializeTopTenAvgPlaytimeForever(data)

	if err != nil {
		log.Errorf("Failed to deserialize top ten avg playtime forever: %v", err)
		return err
	}

	for _, game := range resultList {
		err = u.WriteAllToFile(file, []byte(df.GetStrRepresentation(game)))
		if err != nil {
			log.Errorf("Failed to write to file: %v", err)
			return err
		}
	}

	log.Info("Query saved to top_ten_decade_avg_ptf_query file")
	return nil

}

func handleActionNegativeReviewAbovePercentileQuery(data []byte, name_file string) error {

	file, err := os.OpenFile(name_file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()
	game, err := jr.DeserializeJoinedActionNegativeGameReview(data)
	if err != nil {
		log.Errorf("Failed to deserialize joined action game review: %v", err)
		return err
	}

	err = u.WriteAllToFile(file, []byte(jr.GetStrRepresentationNegativeGameReview(game)))

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to percentile_negative_action_reviews_query file")
	return nil

}

func handleActionEnfglish5kReviewsQuery(data []byte, name_file string) error {

	file, err := os.OpenFile(name_file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()

	game, err := jr.DeserializeJoinedActionGameReview(data)
	if err != nil {
		log.Errorf("Failed to deserialize joined action game review: %v", err)
		return err
	}

	err = u.WriteAllToFile(file, []byte(jr.GetStrRepresentation(game)))

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to action_positive_reviews_query file")
	return nil

}

func handleIndieTopPositiveReviewsQuery(data []byte, name_file string) error {

	file, err := os.OpenFile(name_file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}

	defer file.Close()

	gamesList, err := sp.DeserializeMsgJoinedIndieGameReviewsBatch(data)

	for _, game := range gamesList {
		err = u.WriteAllToFile(file, []byte(jr.GetStrRepresentation(game)))
		if err != nil {
			log.Errorf("Failed to write to file: %v", err)
			return err
		}
	}

	if err != nil {
		log.Errorf("Failed to write to file: %v", err)
		return err
	}
	log.Info("Query saved to action_positive_reviews_query file")
	return nil

}

func sendQueryResult(queryResponseExchange *mom.Exchange, fileName string, queryNumber int) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Errorf("Failed to read file: %v", err)
		return err
	}

	totalLength := len(content)
	var bytesToSend []byte = make([]byte, 3+totalLength)

	bytesToSend[0] = byte(queryNumber)
	binary.BigEndian.PutUint16(bytesToSend[1:], uint16(totalLength))
	copy(bytesToSend[3:], content)

	err = queryResponseExchange.Publish(QueryResponseRoutingKey, bytesToSend)
	if err != nil {
		log.Errorf("Failed to publish file content: %v", err)
		return err
	}

	return nil
}
