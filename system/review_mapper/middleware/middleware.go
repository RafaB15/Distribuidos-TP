package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	mom "distribuidos-tp/middleware"
	"fmt"
	"strconv"
)

const (
	middlewareURI = "amqp://guest:guest@rabbitmq:5672/"

	RawReviewsExchangeName = "raw_reviews_exchange"
	RawReviewsExchangeType = "fanout"
	RawReviewsQueueName    = "raw_reviews_queue"

	ReviewsExchangeName     = "reviews_exchange"
	ReviewsExchangeType     = "direct"
	ReviewsRoutingKeyPrefix = "reviews_key_"

	RawReviewsEofExchangeName    = "raw_reviews_eof_exchange"
	RawReviewsEofExchangeType    = "fanout"
	RawReviewsEofQueueNamePrefix = "raw_reviews_eof_queue_"

	AccumulatorsAmountEnvironmentVariableName = "ACCUMULATORS_AMOUNT"
	IdEnvironmentVariableName                 = "ID"
)

type Middleware struct {
	Manager            *mom.MiddlewareManager
	RawReviewsQueue    *mom.Queue
	RawReviewsEofQueue *mom.Queue
	ReviewsExchange    *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	rawReviewsQueue, err := manager.CreateBoundQueue(RawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType, "", true)
	if err != nil {
		return nil, err
	}

	id, err := u.GetEnv(IdEnvironmentVariableName)
	if err != nil {
		return nil, err
	}
	rawReviewsEofQueueName := fmt.Sprintf("%s%s", RawReviewsEofQueueNamePrefix, id)
	rawReviewsEofQueue, err := manager.CreateBoundQueue(rawReviewsEofQueueName, RawReviewsEofExchangeName, RawReviewsEofExchangeType, "", true)
	if err != nil {
		return nil, err
	}

	reviewsEchange, err := manager.CreateExchange(ReviewsExchangeName, ReviewsExchangeType)
	if err != nil {
		return nil, err
	}

	return &Middleware{
		Manager:            manager,
		RawReviewsQueue:    rawReviewsQueue,
		RawReviewsEofQueue: rawReviewsEofQueue,
		ReviewsExchange:    reviewsEchange,
	}, nil

}

func (m *Middleware) ReceiveReviewBatch() ([]string, bool, error) {
	//timeout := time.Second * 2
	msg, err := m.RawReviewsQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	if msg == nil {
		return nil, true, nil
	}

	messageType, err := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	var lines []string
	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgBatch:
		lines, err = sp.DeserializeBatch(msg)
		if err != nil {
			return nil, false, err
		}
		//d.Ack(false)
		//timeout = time.Second * 2
	/*case <-time.After(timeout):
	eofMsg, err := m.RawReviewsEofQueue.GetIfAvailable()
	if err != nil {
		timeout = time.Second * 2
		//continue
	}
	msgType, err := sp.DeserializeMessageType(eofMsg.Body)
	if err != nil {
		return nil, false, err
	}
	if msgType == sp.MsgEndOfFile {
		return nil, true, nil
	}*/
	default:
		return nil, false, fmt.Errorf("unexpected message type: %v", messageType)

	}

	return lines, false, nil

}

func GetAccumulatorsAmount() (int, error) {
	accumulatorsAmountString, err := u.GetEnv(AccumulatorsAmountEnvironmentVariableName)
	if err != nil {
		return 0, err
	}

	accumulatorsAmount, err := strconv.Atoi(accumulatorsAmountString)
	if err != nil {
		return 0, err
	}

	return accumulatorsAmount, nil
}

func (m *Middleware) SendMetrics(reviewsInformation []*r.Review, routingKey string) error {

	serializedReviews := sp.SerializeMsgReviewInformation(reviewsInformation)

	err := m.ReviewsExchange.Publish(routingKey, serializedReviews)
	if err != nil {
		return err
	}

	return nil
}

func (m *Middleware) SendEof(id int) error {
	routingKey := fmt.Sprintf("%v%d", ReviewsRoutingKeyPrefix, id)
	err := m.ReviewsExchange.Publish(routingKey, sp.SerializeMsgEndOfFile())
	if err != nil {
		return err
	}

	return nil
}
