package percentile_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PercentileAccumulator struct {
	ReceiveGameReviewsMetrics func() ([]*ra.GameReviewsMetrics, bool, error)
	SendGameReviewsMetrics    func(map[string][]*ra.GameReviewsMetrics) error
	SendEndOfFiles            func(int, string) error
}

func NewPercentileAccumulator(receiveGameReviewsMetrics func() ([]*ra.GameReviewsMetrics, bool, error), sendGameReviewsMetrics func(map[string][]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int, string) error) *PercentileAccumulator {
	return &PercentileAccumulator{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendGameReviewsMetrics,
		SendEndOfFiles:            sendEndOfFiles,
	}
}

func (p *PercentileAccumulator) Run(actionNegativeReviewsJoinersAmount int, accumulatedPercentileReviewsRoutingKeyPrefix string, previousAccumulators int, fileName string) {
	remainingEOFs := previousAccumulators

	for {
		accumulatedPercentileKeyMap := make(map[string][]*ra.GameReviewsMetrics)
		gameReviewsMetrics, eof, err := p.ReceiveGameReviewsMetrics()
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
			abovePercentile, err := ra.GetTop10PercentByNegativeReviews(fileName)
			if err != nil {
				log.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
				return
			}
			for _, review := range abovePercentile {
				key := u.GetPartitioningKeyFromInt(int(review.AppID), actionNegativeReviewsJoinersAmount, accumulatedPercentileReviewsRoutingKeyPrefix)
				accumulatedPercentileKeyMap[key] = append(accumulatedPercentileKeyMap[key], review)
				log.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
			}

			p.SendGameReviewsMetrics(accumulatedPercentileKeyMap)
			p.SendEndOfFiles(actionNegativeReviewsJoinersAmount, accumulatedPercentileReviewsRoutingKeyPrefix)
			continue
		}

		err = ra.AddGamesAndMaintainOrder(fileName, gameReviewsMetrics)
		if err != nil {
			log.Errorf("Failed to add games and maintain order: %v", err)
			return
		}

	}
}
