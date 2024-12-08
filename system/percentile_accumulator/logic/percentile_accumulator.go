package percentile_accumulator

import (
	ra "distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/percentile_accumulator/persistence"
	"errors"
	"math"
	"sort"

	"github.com/op/go-logging"
)

const (
	AckBatchSize = 1
)

type ReceiveGameReviewsMetricsFunc func(messageTracker *n.MessageTracker) (clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics, eof bool, newMessage bool, delMessage bool, err error)
type SendQueryResultsFunc func(clientID int, namedGameReviewsMetricsBatch []*ra.NamedGameReviewsMetrics) error
type AckLastMessageFunc func() error

type PercentileAccumulator struct {
	ReceiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc
	SendGameReviewsMetrics    SendQueryResultsFunc
	AckLastMessage            AckLastMessageFunc
	logger                    *logging.Logger
}

func NewPercentileAccumulator(receiveGameReviewsMetrics ReceiveGameReviewsMetricsFunc, sendQueryResults SendQueryResultsFunc, ackLastMessage AckLastMessageFunc, logger *logging.Logger) *PercentileAccumulator {
	return &PercentileAccumulator{
		ReceiveGameReviewsMetrics: receiveGameReviewsMetrics,
		SendGameReviewsMetrics:    sendQueryResults,
		AckLastMessage:            ackLastMessage,
		logger:                    logger,
	}
}

func (p *PercentileAccumulator) Run(previousAccumulators int, repository *p.Repository) {

	percentileMap, messageTracker, syncNumber, err := repository.LoadAll(previousAccumulators)
	if err != nil {
		p.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, gameReviewsMetrics, eof, newMessage, delMessage, err := p.ReceiveGameReviewsMetrics(messageTracker)
		if err != nil {
			p.logger.Errorf("Failed to receive game reviews metrics: %v", err)
			return
		}

		percentileReviews, exists := percentileMap.Get(clientID)
		if !exists {
			percentileReviews = []*ra.NamedGameReviewsMetrics{}
			percentileMap.Set(clientID, percentileReviews)

		}

		if newMessage && !eof && !delMessage {
			percentileReviews, exists := percentileMap.Get(clientID)
			if !exists {
				p.logger.Errorf("Client %d does not exist in the map", clientID)
				return
			}
			allReviews := addGamesAndMaintainOrder(percentileReviews, gameReviewsMetrics)
			percentileMap.Set(clientID, allReviews)
			p.logger.Infof("Received game reviews metrics for client %d", clientID)
			p.logger.Infof("Quantity of games: %d", len(allReviews))
		}

		if delMessage {
			p.logger.Infof("Received delete message for client %d.", clientID)

			messageTracker.DeleteClientInfo(clientID)
			percentileMap.Delete(clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, p.logger)
		if clientFinished {
			p.logger.Infof("Client %d finished sending data", clientID)
			abovePercentile, err := getTop10PercentByNegativeReviews(percentileReviews, p.logger)
			if err != nil {
				p.logger.Errorf("Failed to get top 10 percent by negative reviews: %v", err)
				return
			}
			for _, review := range abovePercentile {
				p.logger.Infof("Metrics above p90: id:%v #:%v", review.AppID, review.NegativeReviews)
			}

			err = p.SendGameReviewsMetrics(clientID, abovePercentile)
			if err != nil {
				p.logger.Errorf("Failed to send game reviews metrics: %v", err)
				return
			}
			messageTracker.DeleteClientInfo(clientID)
			percentileMap.Delete(clientID)
		}

		if messagesUntilAck == 0 || delMessage || clientFinished {
			saves := 1
			if delMessage || clientFinished {
				saves = 2
			}

			for i := 0; i < saves; i++ {
				syncNumber++
				err = repository.SaveAll(percentileMap, messageTracker, syncNumber)
				if err != nil {
					p.logger.Errorf("failed to save data: %v", err)
					return
				}
			}

			messagesUntilAck = AckBatchSize
			err = p.AckLastMessage()
			if err != nil {
				p.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		} else {
			messagesUntilAck--
		}

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
	percentileIndex := int(math.Ceil(0.9*float64(len(games))) - 1)

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
