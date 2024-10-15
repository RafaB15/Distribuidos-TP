package review_mapper

import (
	r "distribuidos-tp/internal/system_protocol/reviews"

	"github.com/op/go-logging"
)

const (
	REVIEW_ID_INDEX = 0
	REVIEW_VOTE_INDEX = 2
)

var log = logging.MustGetLogger("log")

type ReviewMapper struct {
	ReceiveReviewBatch func() ([]string, bool, error)
}

func NewReviewMapper(receiveReviewBatch func() ([]string, bool, error), GetAccumulatorsAmount func() (int, error), SendMetrics func() (error)) *ReviewMapper {
	return &ReviewMapper{
		ReceiveReviewBatch: receiveReviewBatch,
		AccumulatorsAmount: accumulatorsAmount,
		SendMetrics: sendMetrics,
	}
}

func (r *ReviewMapper) Run() {
	routingKeyMap := make(map[string][]*r.Review)

	accumulatorsAmount, err := r.GetAccumulatorsAmount()
	if err != nil {
		log.Errorf("Failed to get accumulators amount: %v", err)
		return
	}

	for {
		reviews, eof, err := r.ReceiveReviewBatch()
		if err != nil {
			log.Errorf("Failed to receive review batch: %v", err)
			return
		}

		if eof {
			// Do something
		}

		for _, review := range reviews {
			records, err := getRecords(review)
			if err != nil {
				log.Errorf("Failed to read review: %v", err)
				continue
			}

			review, err := r.NewReviewFromStrings(records[REVIEW_ID_INDEX], records[REVIEW_VOTE_INDEX]) //este import tambien hay que arreglarlo.
			if err != nil {
				log.Errorf("Failed to create review struct: %v", err)
				continue
			}

			updateReviewsMap(review, routingKeyMap, accumulatorsAmount)

		}

		for routingKey, reviews := range routingKeyMap {
			err := r.SendMetrics(reviews, routingKey)
			if err != nil {
				log.Errorf("Failed to send metrics: %v", err)
				return
			}
		}
	}


}

func getRecords(review string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(review))
	records, err := reader.Read()

	if err != nil {
		log.Errorf("Failed to read review: %v", err)
		return nil, err
	}

	return records, nil
}

func updateReviewsMap(review *r.Review, routingKeyMap map[string][]*r.Review, accumulatorsAmount int) {
	routingKey := u.GetPartitioningKeyFromInt(int(review.AppId), accumulatorsAmount, ReviewsRoutingKeyPrefix)
	routingKeyMap[routingKey] = append(routingKeyMap[routingKey], review)
}

