package review_mapper

import (
	re "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"strings"

	"github.com/op/go-logging"
)

const (
	REVIEW_ID_INDEX   = 0
	REVIEW_VOTE_INDEX = 2

	ReviewsRoutingKeyPrefix = "reviews_key_" //consultar
)

var log = logging.MustGetLogger("log")

type ReviewMapper struct {
	ReceiveReviewBatch func() ([]string, bool, error)
	AccumulatorsAmount func() (int, error)
	SendMetrics        func([]*re.Review, string) error
	SendEof            func(int) error
}

func NewReviewMapper(receiveReviewBatch func() ([]string, bool, error), accumulatorsAmount func() (int, error), sendMetrics func([]*re.Review, string) error, SendEof func(int) error) *ReviewMapper {
	return &ReviewMapper{
		ReceiveReviewBatch: receiveReviewBatch,
		AccumulatorsAmount: accumulatorsAmount,
		SendMetrics:        sendMetrics,
		SendEof:            SendEof,
	}
}

func (r *ReviewMapper) Run() {
	routingKeyMap := make(map[string][]*re.Review)

	accumulatorsAmount, err := r.AccumulatorsAmount()
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
			for i := 0; i < accumulatorsAmount; i++ {
				err = r.SendEof(i)
				if err != nil {
					log.Errorf("Failed to send EOF: %v", err)
					return
				}
			}
			// Do something

		}

		for _, review := range reviews {
			records, err := getRecords(review)
			if err != nil {
				log.Errorf("Failed to read review: %v", err)
				continue
			}

			review, err := re.NewReviewFromStrings(records[REVIEW_ID_INDEX], records[REVIEW_VOTE_INDEX]) //este import tambien hay que arreglarlo.
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

// ver si esto deberis estar aca o no
func updateReviewsMap(review *re.Review, routingKeyMap map[string][]*re.Review, accumulatorsAmount int) {
	routingKey := u.GetPartitioningKeyFromInt(int(review.AppId), accumulatorsAmount, ReviewsRoutingKeyPrefix)
	routingKeyMap[routingKey] = append(routingKeyMap[routingKey], review)
}
