package action_negative_review_joiner

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ActionNegativeReviewJoiner struct {
	ReceiveMsg               func() (sp.MessageType, []byte, bool, error)
	HandleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error)
	HandleGameNames          func([]byte) ([]*games.GameName, error)
	SendMetrics              func(*j.JoinedActionNegativeGameReview) error
	SendEof                  func() error
}

func NewActionNegativeReviewJoiner(receiveMsg func() (sp.MessageType, []byte, bool, error), handleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error), handleGameNames func([]byte) ([]*games.GameName, error), sendMetrics func(*j.JoinedActionNegativeGameReview) error, sendEof func() error) *ActionNegativeReviewJoiner {
	return &ActionNegativeReviewJoiner{
		ReceiveMsg:               receiveMsg,
		HandleGameReviewsMetrics: handleGameReviewsMetrics,
		HandleGameNames:          handleGameNames,
		SendMetrics:              sendMetrics,
		SendEof:                  sendEof,
	}
}

func (a *ActionNegativeReviewJoiner) Run() {
	remainingEOFs := 1 + 1

	accumulatedGameReviews := make(map[uint32]*j.JoinedActionNegativeGameReview)

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

			for _, gameReviewMetrics := range gamesReviewsMetrics {
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
					newJoinedActionGameReview := j.NewJoinedActionNegativeGameReview(actionGameName.AppId)
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
