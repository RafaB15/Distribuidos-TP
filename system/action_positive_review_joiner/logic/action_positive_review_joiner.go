package action_positive_review_joiner

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ActionPositiveReviewJoiner struct {
	ReceiveMsg               func() (sp.MessageType, []byte, bool, error)
	HandleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error)
	HandleGameNames          func([]byte) ([]*games.GameName, error)
	SendMetrics              func(*j.JoinedActionGameReview) error
	SendEof                  func() error
}

func NewActionPositiveReviewJoiner(receiveMsg func() (sp.MessageType, []byte, bool, error), handleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error), handleGameNames func([]byte) ([]*games.GameName, error), sendMetrics func(*j.JoinedActionGameReview) error, sendEof func() error) *ActionPositiveReviewJoiner {
	return &ActionPositiveReviewJoiner{
		ReceiveMsg:               receiveMsg,
		HandleGameReviewsMetrics: handleGameReviewsMetrics,
		HandleGameNames:          handleGameNames,
		SendMetrics:              sendMetrics,
		SendEof:                  sendEof,
	}
}

func (a *ActionPositiveReviewJoiner) Run(positiveReviewsFiltersAmount int) {
	remainingEOFs := positiveReviewsFiltersAmount + 1

	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)

	for {
		msgType, msg, eof, err := a.ReceiveMsg()
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

		switch msgType {
		case sp.MsgGameReviewsMetrics:
			gamesReviewsMetrics, err := a.HandleGameReviewsMetrics(msg)
			if err != nil {
				log.Errorf("Failed to handle game reviews metrics: %v", err)
				return
			}

			for _, gameReviewsMetrics := range gamesReviewsMetrics {
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

		case sp.MsgGameNames:
			actionGamesNames, err := a.HandleGameNames(msg)
			if err != nil {
				log.Errorf("Failed to handle game names: %v", err)
				return
			}

			for _, actionGameName := range actionGamesNames {
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

		default:
			if msgType == sp.MsgEndOfFile {
				continue
			}
			log.Errorf("Unexpected message type: %d", msgType)
		}
	}

}
