package review_mapper

import (
	re "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	APP_ID_INDEX       = 0
	REVIEW_SCORE_INDEX = 2
)

var log = logging.MustGetLogger("log")

type ReviewMapper struct {
	ReceiveGameReviews func() ([]string, bool, error)
	SendReviews        func(map[int][]*re.Review) error
	SendEnfOfFiles     func(int) error
}

func NewReviewMapper(
	receiveReviewBatch func() ([]string, bool, error),
	sendMetrics func(map[int][]*re.Review) error,
	SendEof func(int) error,
) *ReviewMapper {
	return &ReviewMapper{
		ReceiveGameReviews: receiveReviewBatch,
		SendReviews:        sendMetrics,
		SendEnfOfFiles:     SendEof,
	}
}

func (r *ReviewMapper) Run(accumulatorsAmount int) {
	for {
		reviews, eof, err := r.ReceiveGameReviews()
		if err != nil {
			log.Errorf("Failed to receive review batch: %v", err)
			return
		}

		if eof {
			log.Info("Received end of file")
			err = r.SendEnfOfFiles(accumulatorsAmount)
			if err != nil {
				log.Errorf("Failed to send end of files: %v", err)
				return
			}
			continue
		}

		reviewsMap, err := collectReviews(reviews, accumulatorsAmount)
		if err != nil {
			log.Errorf("Failed to handle message batch: %v", err)
			return
		}

		err = r.SendReviews(reviewsMap)
		if err != nil {
			log.Errorf("Failed to send english reviews: %v", err)
			return
		}
		log.Info("Sent reviews")
	}

}

func collectReviews(reviews []string, accumulatorsAmount int) (map[int][]*re.Review, error) {
	shardingKeyMap := make(map[int][]*re.Review)

	for _, line := range reviews {
		reader := csv.NewReader(strings.NewReader(line))
		records, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		review, err := re.NewReviewFromStrings(records[APP_ID_INDEX], records[REVIEW_SCORE_INDEX])
		if err != nil {
			log.Error("Error creating review with text")
			return nil, err
		}

		updateReviewsMap(review, shardingKeyMap, accumulatorsAmount)

	}

	return shardingKeyMap, nil
}

func updateReviewsMap(review *re.Review, routingKeyMap map[int][]*re.Review, accumulatorsAmount int) {
	appIdStr := strconv.Itoa(int(review.AppId))
	englishReviewShardingKey := u.CalculateShardingKey(appIdStr, accumulatorsAmount)
	routingKeyMap[englishReviewShardingKey] = append(routingKeyMap[englishReviewShardingKey], review)
}
