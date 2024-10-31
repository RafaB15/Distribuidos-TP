package negative_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"
	"strconv"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type NegativeReviewsFilter struct {
	ReceiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error)
	SendGameReviewsMetrics    func(int, map[int][]*ra.GameReviewsMetrics) error
	SendEndOfFiles            func(int, int) error
}

func NewNegativeReviewsFilter(receiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error), sendGameReviewsMetrics func(int, map[int][]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int, int) error) *NegativeReviewsFilter {
	return &NegativeReviewsFilter{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendGameReviewsMetrics,
		SendEndOfFiles:            sendEndOfFiles,
	}
}

func (f *NegativeReviewsFilter) Run(actionReviewsJoinersAmount int, englishReviewAccumulatorsAmount int, minNegativeReviews int) {
	remainingEOFsMap := make(map[int]int)
	negativeReviewsMap := make(map[int]map[int][]*ra.GameReviewsMetrics)

	for {

		clientID, gameReviewsMetrics, eof, err := f.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientNegativeReviews, exists := negativeReviewsMap[clientID]
		if !exists {
			clientNegativeReviews = make(map[int][]*ra.GameReviewsMetrics)
			negativeReviewsMap[clientID] = clientNegativeReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)
			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = englishReviewAccumulatorsAmount
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			log.Infof("Received all EOFs for client %d", clientID)
			err = f.SendEndOfFiles(clientID, actionReviewsJoinersAmount)
			if err != nil {
				log.Errorf("Failed to send EOFs: %v", err)
				return
			}

			log.Infof("Sent all EOFs to Joiners from client: %d", clientID)

			delete(negativeReviewsMap, clientID)
			delete(remainingEOFsMap, clientID)

			continue
		}

		for _, gameReviewsMetrics := range gameReviewsMetrics {
			if gameReviewsMetrics.NegativeReviews >= minNegativeReviews {
				log.Infof("Game %v has %v negative reviews", gameReviewsMetrics.AppID, gameReviewsMetrics.NegativeReviews)
				updateNegativeReviewsMap(clientNegativeReviews, gameReviewsMetrics, actionReviewsJoinersAmount)
			}
		}

		err = f.SendGameReviewsMetrics(clientID, clientNegativeReviews)
		if err != nil {
			log.Errorf("Failed to send game reviews metrics: %v", err)
			return
		}
	}
}

func updateNegativeReviewsMap(negativeReviewsMap map[int][]*ra.GameReviewsMetrics, gameReviewsMetrics *ra.GameReviewsMetrics, actionReviewsJoinersAmount int) {
	appIdString := strconv.Itoa(int(gameReviewsMetrics.AppID))
	shardingKey := u.CalculateShardingKey(appIdString, actionReviewsJoinersAmount)
	negativeReviewsMap[shardingKey] = append(negativeReviewsMap[shardingKey], gameReviewsMetrics)
}
