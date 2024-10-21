package action_positive_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ActionPositiveReviewJoiner struct {
	ReceiveMsg  func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendMetrics func(*j.JoinedActionGameReview) error
	SendEof     func() error
}

func NewActionPositiveReviewJoiner(receiveMsg func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error), sendMetrics func(*j.JoinedActionGameReview) error, sendEof func() error) *ActionPositiveReviewJoiner {
	return &ActionPositiveReviewJoiner{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (a *ActionPositiveReviewJoiner) Run(positiveReviewsFiltersAmount int) {
	remainingEOFs := positiveReviewsFiltersAmount + 1

	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)

	for {
		games, reviews, eof, err := a.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		if eof {
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}

			log.Info("Received all EOFs, sending EOF to Writer")
			err = a.SendEof()
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
		}

		if games != nil {
			for _, actionGameName := range games {
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[actionGameName.AppId]; exists {
					log.Infof("Joining action game into review with ID: %v", actionGameName.AppId)
					joinedGameReviewsMsg.UpdateWithGame(actionGameName)

					err = a.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					log.Infof("Sending review for game with ID: %v", actionGameName.AppId)
					// delete the accumulated review
					delete(accumulatedGameReviews, actionGameName.AppId)
				} else {
					log.Infof("Saving action game for later join with id %v", actionGameName.AppId)
					newJoinedActionGameReview := j.NewJoinedActionGameReview(actionGameName.AppId)
					newJoinedActionGameReview.UpdateWithGame(actionGameName)
					accumulatedGameReviews[actionGameName.AppId] = newJoinedActionGameReview
				}
			}
		}

		if reviews != nil {
			for _, gameReviewsMetrics := range reviews {
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					err = a.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					log.Infof("Sent review for game with ID: %v", gameReviewsMetrics.AppID)
					delete(accumulatedGameReviews, gameReviewsMetrics.AppID)
				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
				}
			}
		}
	}

}
