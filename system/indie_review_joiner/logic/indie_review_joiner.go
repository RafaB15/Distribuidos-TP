package indie_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type IndieReviewJoiner struct {
	ReceiveMsg  func() (int, []*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendMetrics func(int, *j.JoinedPositiveGameReview) error
	SendEof     func(int) error
}

func NewIndieReviewJoiner(receiveMsg func() (int, []*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error), sendMetrics func(int, *j.JoinedPositiveGameReview) error, sendEof func(int) error) *IndieReviewJoiner {
	return &IndieReviewJoiner{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (i *IndieReviewJoiner) Run(accumulatorsAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedGameReviews := make(map[int]map[uint32]*j.JoinedPositiveGameReview)

	for {
		clientID, games, reviews, eof, err := i.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedGameReviews, exists := accumulatedGameReviews[clientID]
		if !exists {
			clientAccumulatedGameReviews = make(map[uint32]*j.JoinedPositiveGameReview)
			accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		}

		if eof {
			log.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = accumulatorsAmount + 1
			}
			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs, sending EOFs")
			err = i.SendEof(clientID)
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
		}

		if games != nil {
			for _, indieGame := range games {

				if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[indieGame.AppId]; exists {
					log.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
					joinedGameReviewsMsg.UpdateWithGame(indieGame)
					err = i.SendMetrics(clientID, joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					delete(clientAccumulatedGameReviews, indieGame.AppId)
				} else {
					log.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
					newJoinedPositiveGameReview := j.NewJoinedPositiveGameReview(indieGame.AppId)
					newJoinedPositiveGameReview.UpdateWithGame(indieGame)
					clientAccumulatedGameReviews[indieGame.AppId] = newJoinedPositiveGameReview
				}
			}
			continue
		}

		if reviews != nil {
			for _, gameReviewsMetrics := range reviews {
				if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					err = i.SendMetrics(clientID, joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					delete(clientAccumulatedGameReviews, gameReviewsMetrics.AppID)

				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					clientAccumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedPositiveGameReview(gameReviewsMetrics.AppID)
				}
			}
			continue
		}
	}
}
