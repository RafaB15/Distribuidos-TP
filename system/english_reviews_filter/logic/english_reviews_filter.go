package english_reviews_filter

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	u "distribuidos-tp/internal/utils"
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	APP_ID_INDEX       = 0
	REVIEW_TEXT_INDEX  = 1
	REVIEW_SCORE_INDEX = 2
)

var log = logging.MustGetLogger("log")

type EnglishReviewsFilter struct {
	ReceiveGameReviews func() (int, []string, bool, error)
	SendEnglishReviews func(clientID int, reviewsMap map[int][]*r.Review) error
	SendEnfOfFiles     func(clientID int, accumulatorsAmount int) error
}

func NewEnglishReviewsFilter(
	receiveGameReviews func() (int, []string, bool, error),
	sendEnglishReviews func(int, map[int][]*r.Review) error,
	sendEndOfFiles func(int, int) error,
) *EnglishReviewsFilter {
	return &EnglishReviewsFilter{
		ReceiveGameReviews: receiveGameReviews,
		SendEnglishReviews: sendEnglishReviews,
		SendEnfOfFiles:     sendEndOfFiles,
	}
}

func (f *EnglishReviewsFilter) Run(accumulatorsAmount int) {
	languageIdentifier := r.NewLanguageIdentifier()

	for {
		clientID, reviews, eof, err := f.ReceiveGameReviews()
		if err != nil {
			log.Errorf("Failed to receive game reviews: %v", err)
			return
		}

		if eof {
			log.Info("Received end of file")
			err = f.SendEnfOfFiles(clientID, accumulatorsAmount)
			if err != nil {
				log.Errorf("Failed to send end of files: %v", err)
				return
			}
			continue
		}

		reviewsMap, err := collectReviews(reviews, accumulatorsAmount, languageIdentifier)
		if err != nil {
			log.Errorf("Failed to handle message batch: %v", err)
			return
		}

		err = f.SendEnglishReviews(clientID, reviewsMap)
		if err != nil {
			log.Errorf("Failed to send english reviews: %v", err)
			return
		}
	}
}

func collectReviews(reviews []string, accumulatorsAmount int, languageIdentifier *r.LanguageIdentifier) (map[int][]*r.Review, error) {
	shardingKeyMap := make(map[int][]*r.Review)

	for _, line := range reviews {
		reader := csv.NewReader(strings.NewReader(line))
		records, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		review, err := r.NewReviewFromStrings(records[APP_ID_INDEX], records[REVIEW_SCORE_INDEX])
		if err != nil {
			log.Error("Error creating review with text")
			return nil, err
		}

		if languageIdentifier.IsEnglish(records[REVIEW_TEXT_INDEX]) {
			updateEnglishReviewsMap(review, shardingKeyMap, accumulatorsAmount)
		}
	}

	return shardingKeyMap, nil
}

func updateEnglishReviewsMap(review *r.Review, routingKeyMap map[int][]*r.Review, accumulatorsAmount int) {
	appIdStr := strconv.Itoa(int(review.AppId))
	englishReviewShardingKey := u.CalculateShardingKey(appIdStr, accumulatorsAmount)
	routingKeyMap[englishReviewShardingKey] = append(routingKeyMap[englishReviewShardingKey], review)
}
