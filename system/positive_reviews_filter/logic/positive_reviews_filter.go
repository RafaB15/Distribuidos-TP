package positive_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"
	"strconv"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PositiveReviewsFilter struct {
	ReceiveGameReviewsMetrics func() ([]*ra.GameReviewsMetrics, bool, error)
	SendGameReviewsMetrics    func(map[int][]*ra.GameReviewsMetrics) error
	SendEndOfFiles            func(int) error
}

func NewPositiveReviewsFilter(receiveGameReviewsMetrics func() ([]*ra.GameReviewsMetrics, bool, error), sendGameReviewsMetrics func(map[int][]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int) error) *PositiveReviewsFilter {
	return &PositiveReviewsFilter{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendGameReviewsMetrics,
		SendEndOfFiles:            sendEndOfFiles,
	}
}

func (f *PositiveReviewsFilter) Run(actionReviewsJoinersAmount int, englishReviewAccumulatorsAmount int, minPositiveReviews int) {
	remainingEOFs := actionReviewsJoinersAmount

	for {
		positiveReviewsMap := make(map[int][]*ra.GameReviewsMetrics)

		gameReviewsMetrics, eof, err := f.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		if eof {
			remainingEOFs--
			log.Info("Received EOF")
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs")
			f.SendEndOfFiles(actionReviewsJoinersAmount)
			continue
		}

		for _, gameReviewsMetrics := range gameReviewsMetrics {
			if gameReviewsMetrics.PositiveReviews >= minPositiveReviews {
				log.Infof("Game %v has %v positive reviews", gameReviewsMetrics.AppID, gameReviewsMetrics.PositiveReviews)
				updatePositiveReviewsMap(positiveReviewsMap, gameReviewsMetrics, englishReviewAccumulatorsAmount)
			}
		}

		f.SendGameReviewsMetrics(positiveReviewsMap)
	}
}

func updatePositiveReviewsMap(positiveReviewsMap map[int][]*ra.GameReviewsMetrics, gameReviewsMetrics *ra.GameReviewsMetrics, englishReviewAccumulatorsAmount int) {
	appIdString := strconv.Itoa(int(gameReviewsMetrics.AppID))
	shardingKey := u.CalculateShardingKey(appIdString, englishReviewAccumulatorsAmount)
	positiveReviewsMap[shardingKey] = append(positiveReviewsMap[shardingKey], gameReviewsMetrics)
}
