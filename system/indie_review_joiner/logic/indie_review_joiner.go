package indie_review_joiner

import (
	sp "distribuidos-tp/internal/system_protocol"
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type IndieReviewJoiner struct {
	ReceiveMsg               func() (sp.MessageType, []byte, bool, error)
	HandleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error)
	HandleGameNames          func([]byte) ([]*games.GameName, error)
	SendMetrics              func(*j.JoinedActionGameReview) error
	SendEof                  func() error
}

func NewIndieReviewJoiner(receiveMsg func() (sp.MessageType, []byte, bool, error), handleGameReviewsMetrics func([]byte) ([]*reviews_accumulator.GameReviewsMetrics, error), handleGameNames func([]byte) ([]*games.GameName, error), sendMetrics func(*j.JoinedActionGameReview) error, sendEof func() error) *IndieReviewJoiner {
	return &IndieReviewJoiner{
		ReceiveMsg:               receiveMsg,
		HandleGameReviewsMetrics: handleGameReviewsMetrics,
		HandleGameNames:          handleGameNames,
		SendMetrics:              sendMetrics,
		SendEof:                  sendEof,
	}
}

func (i *IndieReviewJoiner) Run(accumulatorsAmount int) {
	accumulatedGameReviews := make(map[uint32]*j.JoinedActionGameReview)

	remainingEOFs := accumulatorsAmount + 1

	for {
		msgType, msg, eof, err := i.ReceiveMsg()
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

		switch msgType {
		case sp.MsgGameReviewsMetrics:
			gamesReviewsMetrics, err := i.HandleGameReviewsMetrics(msg)
			if err != nil {
				log.Errorf("Failed to handle game reviews metrics: %v", err)
				return
			}

			for _, gameReviewsMetrics := range gamesReviewsMetrics {
				if err != nil {
					log.Errorf("Failed to deserialize game reviews metrics: %v", err)
					return
				}
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
					log.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
					joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

					err = i.SendMetrics(joinedGameReviewsMsg)
					if err != nil {
						log.Errorf("Failed to send metrics: %v", err)
						return
					}

				} else {
					log.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
					accumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedActionGameReview(gameReviewsMetrics.AppID)
				}
			}

			// handle game metrics
		case sp.MsgGameNames:
			indieGamesNames, err := i.HandleGameNames(msg)
			if err != nil {
				log.Errorf("Failed to handle game names: %v", err)
				return
			}

			for _, indieGame := range indieGamesNames {
				if err != nil {
					log.Errorf("Failed to deserialize indie game: %v", err)
					return
				}
				if joinedGameReviewsMsg, exists := accumulatedGameReviews[indieGame.AppId]; exists {
					log.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
					joinedGameReviewsMsg.UpdateWithGame(indieGame)
					err = i.SendMetrics(joinedGameReviewsMsg)
					delete(accumulatedGameReviews, indieGame.AppId)
				} else {
					log.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
					newJoinedActionGameReview := j.NewJoinedActionGameReview(indieGame.AppId)
					newJoinedActionGameReview.UpdateWithGame(indieGame)
					accumulatedGameReviews[indieGame.AppId] = newJoinedActionGameReview
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
