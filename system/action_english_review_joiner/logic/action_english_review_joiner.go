package action_english_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ActionEnglishReviewJoiner struct {
	ReceiveMsg  func() (int, []*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error)
	SendMetrics func(int, *j.JoinedNegativeGameReview) error
	SendEof     func(int) error
}

func NewActionEnglishReviewJoiner(receiveMsg func() (int, []*games.GameName, []*reviews_accumulator.GameReviewsMetrics, bool, error), sendMetrics func(int, *j.JoinedNegativeGameReview) error, sendEof func(int) error) *ActionEnglishReviewJoiner {
	return &ActionEnglishReviewJoiner{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (a *ActionEnglishReviewJoiner) Run(negativeReviewsFiltersAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedGameReviews := make(map[int]map[uint32]*j.JoinedNegativeGameReview)

	for {
		clientID, games, reviews, eof, err := a.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedGameReviews, exists := accumulatedGameReviews[clientID]
		if !exists {
			clientAccumulatedGameReviews = make(map[uint32]*j.JoinedNegativeGameReview)
			accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		}

		if eof {

			log.Info("Received EOF for client ", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = negativeReviewsFiltersAmount + 1
			}

			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}

			log.Infof("Received all EOFs of client: %d, sending EOF to Final Positive Joiner", clientID)
			err = a.SendEof(clientID)
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
			delete(accumulatedGameReviews, clientID)
			delete(remainingEOFsMap, clientID)
		}

		if games != nil {
			for _, actionGameName := range games {
				if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[actionGameName.AppId]; exists {
					log.Infof("Joining action game into review with ID: %v", actionGameName.AppId)
					joinedGameReviewsMsg.UpdateWithGame(actionGameName)

					err = a.SendMetrics(clientID, joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					log.Infof("Sending review for game with ID: %v", actionGameName.AppId)
					// delete the accumulated review
					delete(clientAccumulatedGameReviews, actionGameName.AppId)
				} else {
					log.Infof("Saving action game for later join with id %v", actionGameName.AppId)
					newJoinedNegativeGameReview := j.NewJoinedActionNegativeGameReview(actionGameName.AppId)
					newJoinedNegativeGameReview.UpdateWithGame(actionGameName)
					clientAccumulatedGameReviews[actionGameName.AppId] = newJoinedNegativeGameReview
				}
			}
		}

		if reviews != nil {
			for _, gameReviewsMetrics := range reviews {
				if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					err = a.SendMetrics(clientID, joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}
					log.Infof("Sent review for game with ID: %v", gameReviewsMetrics.AppID)
					delete(clientAccumulatedGameReviews, gameReviewsMetrics.AppID)
				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					clientAccumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionNegativeGameReview(gameReviewsMetrics.AppID)
				}
			}
		}
	}

}