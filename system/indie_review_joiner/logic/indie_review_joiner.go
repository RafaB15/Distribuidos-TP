package indie_review_joiner

import (
	"distribuidos-tp/internal/system_protocol/accumulator/reviews_accumulator"
	"distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/indie_review_joiner/persistence"

	"github.com/op/go-logging"
)

const (
	AckBatchSize  = 25
	GameMapperEOF = 1
)

type ReceiveMsgFunc func(messageTracker *n.MessageTracker) (clientID int, games []*games.GameName, reviews []*reviews_accumulator.GameReviewsMetrics, eof bool, newMessage bool, delMessage bool, e error)
type SendMetricsFunc func(clientID int, metrics *j.JoinedPositiveGameReview, messageTracker *n.MessageTracker) error
type SendEofFunc func(clientID int, senderID int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int) error
type AckLastMessageFunc func() error

type IndieReviewJoiner struct {
	ReceiveMsg       ReceiveMsgFunc
	SendMetrics      SendMetricsFunc
	SendEof          SendEofFunc
	SendDeleteClient SendDeleteClientFunc
	AckLastMessage   AckLastMessageFunc
	logger           *logging.Logger
}

func NewIndieReviewJoiner(receiveMsg ReceiveMsgFunc, sendMetrics SendMetricsFunc, sendEof SendEofFunc, SendDeleteClient SendDeleteClientFunc, ackLastMessage AckLastMessageFunc, logger *logging.Logger) *IndieReviewJoiner {
	return &IndieReviewJoiner{
		ReceiveMsg:       receiveMsg,
		SendMetrics:      sendMetrics,
		SendEof:          sendEof,
		SendDeleteClient: SendDeleteClient,
		AckLastMessage:   ackLastMessage,
		logger:           logger,
	}
}

func (i *IndieReviewJoiner) Run(id int, repository *p.Repository, accumulatorsAmount int) {
	accumulatedGameReviews, gamesToSendMap, messageTracker, syncNumber, err := repository.LoadAll(accumulatorsAmount + GameMapperEOF)
	if err != nil {
		i.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, games, reviews, eof, newMessage, delMessage, err := i.ReceiveMsg(messageTracker)
		if err != nil {
			i.logger.Errorf("Failed to receive message: %v", err)
			return
		}

		if newMessage && !eof && !delMessage {

			clientAccumulatedGameReviews, exists := accumulatedGameReviews.Get(clientID)
			if !exists {
				clientAccumulatedGameReviews = repository.InitializeIndieJoinedReviewsMap()
				accumulatedGameReviews.Set(clientID, clientAccumulatedGameReviews)

			}

			clientGamesToSend, exists := gamesToSendMap.Get(clientID)
			if !exists {
				clientGamesToSend = repository.InitializeGamesToSendMap()
				gamesToSendMap.Set(clientID, clientGamesToSend)
			}

			if games != nil {
				err := i.handleGames(clientID, games, clientGamesToSend, clientAccumulatedGameReviews, messageTracker)
				if err != nil {
					i.logger.Errorf("Failed to handle games: %v", err)
					return
				}
			}

			if reviews != nil {
				err = i.handleReviews(clientID, reviews, clientAccumulatedGameReviews, clientGamesToSend, messageTracker)
				if err != nil {
					i.logger.Errorf("Failed to handle reviews: %v", err)
					return
				}
			}
		}

		if delMessage {
			i.logger.Infof("Deleting client %d information", clientID)

			err = i.SendDeleteClient(clientID)
			if err != nil {
				i.logger.Errorf("Failed to send delete client: %v", err)
				return
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

		}

		if messageTracker.ClientFinished(clientID, i.logger) || delMessage {

			accumulatedGameReviews.Delete(clientID)
			gamesToSendMap.Delete(clientID)
			messageTracker.DeleteClientInfo(clientID)

			syncNumber++
			err := repository.SaveAll(accumulatedGameReviews, gamesToSendMap, messageTracker, syncNumber)
			if err != nil {
				i.logger.Errorf("Failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = i.AckLastMessage()
			if err != nil {
				i.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
			syncNumber++
			err := repository.SaveAll(accumulatedGameReviews, gamesToSendMap, messageTracker, syncNumber)
			if err != nil {
				i.logger.Errorf("Failed to save data: %v", err)
				return
			}

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

func (i *IndieReviewJoiner) handleReviews(clientID int, reviews []*reviews_accumulator.GameReviewsMetrics, clientAccumulatedGameReviews *n.IntMap[*reviews_accumulator.GameReviewsMetrics], clientGamesToSend *n.IntMap[*j.GameToSend], messageTracker *n.MessageTracker) error {
	for _, gameReviewMetric := range reviews {
		clientAccumulatedGameReviews.Set(int(gameReviewMetric.AppID), gameReviewMetric)
		if gameToSend, exists := clientGamesToSend.Get(int(gameReviewMetric.AppID)); exists {
			i.logger.Infof("Joining review into indie game with ID: %v", gameReviewMetric.AppID)
			newJoinedIndieGameReview := j.NewJoinedPositiveGameReview(gameToSend.AppId)
			newJoinedIndieGameReview.UpdateWithReview(gameReviewMetric)
			newJoinedIndieGameReview.UpdateWithGame(gameToSend.Name)
			err := i.SendMetrics(clientID, newJoinedIndieGameReview, messageTracker)
			if err != nil {
				i.logger.Errorf("Failed to send metrics: %v", err)
				return err
			}
			i.logger.Infof("Sent metrics for game with ID %v", gameReviewMetric.AppID)
			clientAccumulatedGameReviews.Delete(int(gameReviewMetric.AppID))
		} else {
			i.logger.Infof("Game with ID %v not found in gamesToSendMap. Saving Review for later join", gameReviewMetric.AppID)
		}
	}
	return nil
}

func (i *IndieReviewJoiner) handleGames(clientID int, games []*games.GameName, clientGamesToSend *n.IntMap[*j.GameToSend], clientAccumulatedGameReviews *n.IntMap[*reviews_accumulator.GameReviewsMetrics], messageTracker *n.MessageTracker) error {
	for _, indieGame := range games {
		gameToSend := j.NewGameToSend(indieGame.AppId, indieGame.Name, true) //asumo que todos los juegos que me estan llegando son indies.
		clientGamesToSend.Set(int(indieGame.AppId), gameToSend)
		if joinedGameReview, exists := clientAccumulatedGameReviews.Get(int(gameToSend.AppId)); exists {
			i.logger.Infof("Joining indie game into review with ID: %v", indieGame.AppId)
			newJoinedIndieGameReview := j.NewJoinedPositiveGameReview(gameToSend.AppId)
			newJoinedIndieGameReview.UpdateWithGame(indieGame.Name)
			newJoinedIndieGameReview.UpdateWithReview(joinedGameReview)
			err := i.SendMetrics(clientID, newJoinedIndieGameReview, messageTracker)
			if err != nil {
				i.logger.Errorf("Failed to send metrics: %v", err)
				return err
			}
			clientAccumulatedGameReviews.Delete(int(gameToSend.AppId))
		} else {
			i.logger.Info("Saving indie game with AppID %v for later join", indieGame.AppId)
		}

	}

	return nil
}
