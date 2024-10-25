package percentile_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"
	"strconv"

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

		fileName := fileNamePrefix + strconv.Itoa(clientID)

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

			abovePercentile, err := ra.GetTop10PercentByNegativeReviews(fileName)
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

		err = ra.AddGamesAndMaintainOrder(fileName, gameReviewsMetrics)
		if err != nil {
			log.Errorf("Failed to add games and maintain order: %v", err)
			return
		}

	}
}
