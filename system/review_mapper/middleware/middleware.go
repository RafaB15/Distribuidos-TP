package middleware

import (
	sp "distribuidos-tp/internal/system_protocol"
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
	Manager              *mom.MiddlewareManager
	RawReviewsQueue      *mom.Queue
	RawReviewsEofQueue   *mom.Queue
	ReviewsExchange      *mom.Exchange
}

func NewMiddleware() (*Middleware, error) {
	manager, err := mom.NewMiddlewareManager(middlewareURI)
	if err != nil {
		return nil, err
	}

	rawReviewsQueue, err := manager.CreateBoundQueue(RawReviewsQueueName, RawReviewsExchangeName, RawReviewsExchangeType,"", true)
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
		Manager:              manager,
		RawReviewsQueue:      rawReviewsQueue,
		RawReviewsEofQueue:   rawReviewsEofQueue,
		ReviewsExchange:      reviewsEchange,
	}, nil

}

func (m *Middleware) ReceiveReviewBatch() ([]string, bool, error) {
	msg, err := m.RawReviewsQueue.Consume()
	if err != nil {
		return nil, false, err
	}

	if msg == nil {
		return nil, true, nil
	}

	messageType := sp.DeserializeMessageType(msg)
	if err != nil {
		return nil, false, err
	}

	var lines []string
	switch messageType {
	case sp.MsgEndOfFile:
		return nil, true, nil
	case sp.MsgBatch:
		lines, err := sp.DeserializeBatch(msg)
		if err != nil {
			return nil, false, err
		}
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

// ya lo arreglo el tipo de dato de reviewsInformation
func (m *Middleware) SendMetrics(reviewsInformation *type.ofReviewSerialization, routingKey string) error {

	serializedReviews := sp.SerializeMsgReviewInformation(reviews)

	err := m.ReviewsExchange.Publish(routingKey, serializedReviews)
	if err != nil {
		return err
	}

	return nil
}