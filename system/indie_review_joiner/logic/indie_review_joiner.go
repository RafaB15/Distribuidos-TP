package indie_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	AckBatchSize  = 50
	GameMapperEOF = 1
)

type ReceiveMsgFunc func(messageTracker *n.MessageTracker) (clientID int, games []*games.GameName, reviews []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, e error)
type SendMetricsFunc func(clientID int, metrics *j.JoinedPositiveGameReview, messageTracker *n.MessageTracker) error
type SendEofFunc func(clientID int, senderID int, messageTracker *n.MessageTracker) error
type AckLastMessageFunc func() error

type IndieReviewJoiner struct {
	ReceiveMsg     ReceiveMsgFunc
	SendMetrics    SendMetricsFunc
	SendEof        SendEofFunc
	AckLastMessage AckLastMessageFunc
	logger         *logging.Logger
}

func NewIndieReviewJoiner(receiveMsg ReceiveMsgFunc, sendMetrics SendMetricsFunc, sendEof SendEofFunc, ackLastMessage AckLastMessageFunc, logger *logging.Logger) *IndieReviewJoiner {
	return &IndieReviewJoiner{
		ReceiveMsg:     receiveMsg,
		SendMetrics:    sendMetrics,
		SendEof:        sendEof,
		AckLastMessage: ackLastMessage,
		logger:         logger,
	}
}

func (i *IndieReviewJoiner) Run(id int, accumulatorsAmount int) {
	messageTracker := n.NewMessageTracker(accumulatorsAmount + GameMapperEOF)
	messagesUntilAck := AckBatchSize
	accumulatedGameReviews := make(map[int]map[uint32]*j.JoinedPositiveGameReview)

	for {
		clientID, games, reviews, eof, newMessage, err := i.ReceiveMsg(messageTracker)
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedGameReviews, exists := accumulatedGameReviews[clientID]
		if !exists {
			clientAccumulatedGameReviews = make(map[uint32]*j.JoinedPositiveGameReview)
			accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		}

		if newMessage && !eof {
			if games != nil {
				err := i.handleGames(clientID, games, clientAccumulatedGameReviews, messageTracker)
				if err != nil {
					i.logger.Errorf("Failed to handle games: %v", err)
					return
				}
			}

			if reviews != nil {
				err = i.handleReview(clientID, reviews, clientAccumulatedGameReviews, messageTracker)
				if err != nil {
					i.logger.Errorf("Failed to handle reviews: %v", err)
					return
				}
			}
		}

		if messageTracker.ClientFinished(clientID, i.logger) {
			i.logger.Infof("Client %d finished", clientID)

			i.logger.Info("Sending EOFs")
			err = i.SendEof(clientID, id, messageTracker)
			if err != nil {
				i.logger.Errorf("Failed to send EOF: %v", err)
				return
			}
			delete(accumulatedGameReviews, clientID)
			messageTracker.DeleteClientInfo(clientID)

			messagesUntilAck = AckBatchSize
			err = i.AckLastMessage()
			if err != nil {
				i.logger.Errorf("Failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {
			err = i.AckLastMessage()
			if err != nil {
				i.logger.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}

func (i *IndieReviewJoiner) handleGames(clientID int, games []*games.GameName, clientAccumulatedGameReviews map[uint32]*j.JoinedPositiveGameReview, messageTracker *n.MessageTracker) error {
	for _, indieGame := range games {

		if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[indieGame.AppId]; exists {
			i.logger.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
			joinedGameReviewsMsg.UpdateWithGame(indieGame)
			err := i.SendMetrics(clientID, joinedGameReviewsMsg, messageTracker)
			if err != nil {
				i.logger.Errorf("Failed to send metrics: %v", err)
				return err
			}
			delete(clientAccumulatedGameReviews, indieGame.AppId)
		} else {
			i.logger.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
			newJoinedPositiveGameReview := j.NewJoinedPositiveGameReview(indieGame.AppId)
			newJoinedPositiveGameReview.UpdateWithGame(indieGame)
			clientAccumulatedGameReviews[indieGame.AppId] = newJoinedPositiveGameReview
		}
	}
	return nil
}

func (i *IndieReviewJoiner) handleReview(clientID int, reviews []*reviews_accumulator.GameReviewsMetrics, clientAccumulatedGameReviews map[uint32]*j.JoinedPositiveGameReview, messageTracker *n.MessageTracker) error {
	for _, gameReviewsMetrics := range reviews {
		if joinedGameReviewsMsg, exists := clientAccumulatedGameReviews[gameReviewsMetrics.AppID]; exists {
			i.logger.Infof("Joining review into indie game with ID: %v", gameReviewsMetrics.AppID)
			joinedGameReviewsMsg.UpdateWithReview(gameReviewsMetrics)

			err := i.SendMetrics(clientID, joinedGameReviewsMsg, messageTracker)
			if err != nil {
				i.logger.Errorf("Failed to send metrics: %v", err)
				return err
			}
			delete(clientAccumulatedGameReviews, gameReviewsMetrics.AppID)

		} else {
			i.logger.Infof("Saving review with AppID %v for later join", gameReviewsMetrics.AppID)
			clientAccumulatedGameReviews[gameReviewsMetrics.AppID] = j.NewJoinedPositiveGameReview(gameReviewsMetrics.AppID)
		}
	}
	return nil
}
