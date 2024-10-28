package percentile_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PercentileAccumulator struct {
	ReceiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error)
	SendGameReviewsMetrics    func(int, map[string][]*ra.GameReviewsMetrics) error
	SendEndOfFiles            func(int, int, string) error
}

func NewPercentileAccumulator(receiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error), sendGameReviewsMetrics func(int, map[string][]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int, int, string) error) *PercentileAccumulator {
	return &PercentileAccumulator{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendGameReviewsMetrics,
		SendEndOfFiles:            sendEndOfFiles,
	}
}

func (p *PercentileAccumulator) Run(actionNegativeReviewsJoinersAmount int, accumulatedPercentileReviewsRoutingKeyPrefix string, previousAccumulators int, fileNamePrefix string) {
	remainingEOFsMap := make(map[int]int)
	accumulatedPercentileKeyMap := make(map[int]map[string][]*ra.GameReviewsMetrics)
	percentileMap := make(map[int][]*ra.GameReviewsMetrics)

	for {
		clientID, gameReviewsMetrics, eof, err := p.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientAccumulatedPercentileKeyMap, exists := accumulatedPercentileKeyMap[clientID]
		if !exists {
			clientAccumulatedPercentileKeyMap = make(map[string][]*ra.GameReviewsMetrics)
			accumulatedPercentileKeyMap[clientID] = clientAccumulatedPercentileKeyMap
		}

		percentileReviews, exists := percentileMap[clientID]
		if !exists {
			percentileReviews = []*ra.GameReviewsMetrics{}
			percentileMap[clientID] = percentileReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = previousAccumulators
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs")

			abovePercentile, err := ra.GetTop10PercentByNegativeReviewsV2(percentileReviews)
			if err != nil {
				log.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
				return
			}
			for _, review := range abovePercentile {
				key := u.GetPartitioningKeyFromInt(int(review.AppID), actionNegativeReviewsJoinersAmount, accumulatedPercentileReviewsRoutingKeyPrefix)
				clientAccumulatedPercentileKeyMap[key] = append(clientAccumulatedPercentileKeyMap[key], review)
				log.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
			}

			p.SendGameReviewsMetrics(clientID, clientAccumulatedPercentileKeyMap)
			p.SendEndOfFiles(clientID, actionNegativeReviewsJoinersAmount, accumulatedPercentileReviewsRoutingKeyPrefix)

			delete(accumulatedPercentileKeyMap, clientID)
			delete(remainingEOFsMap, clientID)

			continue
		}

		allReviews := ra.AddGamesAndMaintainOrderV2(percentileMap[clientID], gameReviewsMetrics)
		percentileMap[clientID] = allReviews
		log.Infof("Received game reviews metrics for client %d", clientID)
		log.Infof("Quantity of games: %d", len(allReviews))

	}
}
