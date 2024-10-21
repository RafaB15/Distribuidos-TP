package action_negative_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ActionNegativeReviewJoiner struct {
	ReceiveMsg  func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendMetrics func(*j.JoinedActionNegativeGameReview) error
	SendEof     func() error
}

func NewActionNegativeReviewJoiner(receiveMsg func() ([]*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error), sendMetrics func(*j.JoinedActionNegativeGameReview) error, sendEof func() error) *ActionNegativeReviewJoiner {
	return &ActionNegativeReviewJoiner{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (a *ActionNegativeReviewJoiner) Run() {
	remainingEOFs := 1 + 1

	accumulatedGameReviews := make(map[uint32]*j.JoinedActionNegativeGameReview)

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
					newJoinedActionGameReview := j.NewJoinedActionNegativeGameReview(actionGameName.AppId)
					newJoinedActionGameReview.UpdateWithGame(actionGameName)
					accumulatedGameReviews[actionGameName.AppId] = newJoinedActionGameReview
				}

			}
		}

		if reviews != nil {
			for _, gameReviewMetrics := range reviews {
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewMetrics.AppID]; exists {
					log.Infof("Joining review into action game with ID: %v", gameReviewMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewMetrics)

					err = a.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					log.Infof("Sent review for game with ID: %v", gameReviewMetrics.AppID)
					// delete the accumulated review
					delete(accumulatedGameReviews, gameReviewMetrics.AppID)
				} else {
					log.Infof("Saving review for later join with id %v", gameReviewMetrics.AppID)
					accumulatedGameReviews[gameReviewMetrics.AppID] = j.NewJoinedActionNegativeGameReview(gameReviewMetrics.AppID)
				}
			}
		}
	}
}
