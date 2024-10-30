package positive_reviews_filter

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	u "distribuidos-tp/internal/utils"
	"strconv"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PositiveReviewsFilter struct {
	ReceiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error)
	SendGameReviewsMetrics    func(int, map[int][]*ra.GameReviewsMetrics) error
	SendEndOfFiles            func(int, int) error
}

func NewPositiveReviewsFilter(receiveGameReviewsMetrics func() (int, []*ra.GameReviewsMetrics, bool, error), sendGameReviewsMetrics func(int, map[int][]*ra.GameReviewsMetrics) error, sendEndOfFiles func(int, int) error) *PositiveReviewsFilter {
	return &PositiveReviewsFilter{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendGameReviewsMetrics,
		SendEndOfFiles:            sendEndOfFiles,
	}
}

func (f *PositiveReviewsFilter) Run(actionReviewsJoinersAmount int, englishReviewAccumulatorsAmount int, minPositiveReviews int) {
	remainingEOFsMap := make(map[int]int)
	positiveReviewsMap := make(map[int]map[int][]*ra.GameReviewsMetrics)

	for {

		clientID, gameReviewsMetrics, eof, err := f.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		clientPositiveReviews, exists := positiveReviewsMap[clientID]
		if !exists {
			clientPositiveReviews = make(map[int][]*ra.GameReviewsMetrics)
			positiveReviewsMap[clientID] = clientPositiveReviews
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
			f.SendEndOfFiles(clientID, actionReviewsJoinersAmount)
			log.Infof("Sent all EOFs to Joiners from client: %d", clientID)

			delete(positiveReviewsMap, clientID)
			delete(remainingEOFsMap, clientID)

			continue
		}

		for _, gameReviewsMetrics := range gameReviewsMetrics {
			if gameReviewsMetrics.PositiveReviews >= minPositiveReviews {
				log.Infof("Game %v has %v positive reviews", gameReviewsMetrics.AppID, gameReviewsMetrics.PositiveReviews)
				updatePositiveReviewsMap(clientPositiveReviews, gameReviewsMetrics, actionReviewsJoinersAmount)
			}
		}

		f.SendGameReviewsMetrics(clientID, clientPositiveReviews)
	}
}

func updatePositiveReviewsMap(positiveReviewsMap map[int][]*ra.GameReviewsMetrics, gameReviewsMetrics *ra.GameReviewsMetrics, actionReviewsJoinersAmount int) {
	appIdString := strconv.Itoa(int(gameReviewsMetrics.AppID))
	shardingKey := u.CalculateShardingKey(appIdString, actionReviewsJoinersAmount)
	positiveReviewsMap[shardingKey] = append(positiveReviewsMap[shardingKey], gameReviewsMetrics)
}
