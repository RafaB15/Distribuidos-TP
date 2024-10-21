package indie_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type IndieReviewJoiner struct {
	ReceiveMsg  func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendMetrics func(*j.JoinedActionGameReview) error
	SendEof     func() error
}

func NewIndieReviewJoiner(receiveMsg func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error), sendMetrics func(*j.JoinedActionGameReview) error, sendEof func() error) *IndieReviewJoiner {
	return &IndieReviewJoiner{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (i *IndieReviewJoiner) Run(accumulatorsAmount int) {
	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)

	remainingEOFs := accumulatorsAmount + 1

	for {
		games, reviews, eof, err := i.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		if eof {
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}
			log.Info("Received all EOFs, sending EOFs")
			err = i.SendEof()
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
		}

		if games != nil {
			for _, indieGame := range games {

				if joinedGameReviewsMsg, exists := accumulatedGameReviews[indieGame.AppId]; exists {
					log.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
					joinedGameReviewsMsg.UpdateWithGame(indieGame)
					err = i.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					delete(accumulatedGameReviews, indieGame.AppId)
				} else {
					log.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
					newJoinedActionGameReview := j.NewJoinedActionGameReview(indieGame.AppId)
					newJoinedActionGameReview.UpdateWithGame(indieGame)
					accumulatedGameReviews[indieGame.AppId] = newJoinedActionGameReview
				}
			}
		}

		if reviews != nil {
			for _, gameReviewsMetrics := range reviews {
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					err = i.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					delete(accumulatedGameReviews, gameReviewsMetrics.AppID)

				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
				}
			}
		}
	}
}
