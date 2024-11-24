package percentile_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"errors"
	"math"
	"sort"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReceiveGameReviewsMetricsFunc func() (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, err error)
type SendQueryResultsFunc func(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error

type PercentileAccumulator struct {
	ReceiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc
	SendGameReviewsMetrics    SendQueryResultsFunc
}

func NewPercentileAccumulator(receiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc, sendQueryResults SendQueryResultsFunc) *PercentileAccumulator {
	return &PercentileAccumulator{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendQueryResults,
	}
}

func (p *PercentileAccumulator) Run(previousAccumulators int) {
	remainingEOFsMap := make(map[int]int)
	percentileMap := make(map[int][]*ra.NamedGameReviewsMetrics)

	for {
		clientID, gameReviewsMetrics, eof, err := p.ReceiveGameReviewsMetrics()
		if err != nil {
			log.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		percentileReviews, exists := percentileMap[clientID]
		if !exists {
			percentileReviews = []*ra.NamedGameReviewsMetrics{}
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

			abovePercentile, err := getTop10PercentByNegativeReviews(percentileReviews, log)
			if err != nil {
				log.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
				return
			}
			for _, review := range abovePercentile {
				log.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
			}

			err = p.SendGameReviewsMetrics(clientID, abovePercentile)
			if err != nil {
				log.Errorf("Failed to send game reviews metrics: %v", err)
				return
			}

			delete(remainingEOFsMap, clientID)

			continue
		}

		allReviews := addGamesAndMaintainOrder(percentileMap[clientID], gameReviewsMetrics)
		percentileMap[clientID] = allReviews
		log.Infof("Received game reviews metrics for client %d", clientID)
		log.Infof("Quantity of games: %d", len(allReviews))

	}
}

func getTop10PercentByNegativeReviews(games []*ra.NamedGameReviewsMetrics, logger *logging.Logger) ([]*ra.NamedGameReviewsMetrics, error) {
	// Log the length of the games slice
	logger.Infof("Total number of games: %d\n", len(games))

	// Si no hay juegos, devolver error
	if len(games) == 0 {
		return nil, errors.New("no games found in file")
	}

	// Calcular la posición del percentil 90
	percentileIndex := int(math.Floor(0.9 * float64(len(games))))

	logger.Infof("Reviews must have more than %d negative reviews to be considered\n", games[percentileIndex].NegativeReviews)

	// Retornar solo los juegos que están por encima del percentil 90

	// Poner en una lista los juegos que están por encima del percentil 90
	// y retornarla
	overPercentile := make([]*ra.NamedGameReviewsMetrics, 0)
	for _, game := range games {
		if game.NegativeReviews >= games[percentileIndex].NegativeReviews {
			overPercentile = append(overPercentile, game)
		}
	}

	return overPercentile, nil
}

func addGamesAndMaintainOrder(existingGames []*ra.NamedGameReviewsMetrics, newGames []*ra.NamedGameReviewsMetrics) []*ra.NamedGameReviewsMetrics {
	// Filter out games with zero negative reviews
	filteredNewGames := make([]*ra.NamedGameReviewsMetrics, 0)
	for _, game := range newGames {
		if game.NegativeReviews > 0 {
			filteredNewGames = append(filteredNewGames, game)
		}
	}

	allGames := append(existingGames, filteredNewGames...)

	sort.Slice(allGames, func(i, j int) bool {
		return allGames[i].NegativeReviews < allGames[j].NegativeReviews
	})

	return allGames
}
