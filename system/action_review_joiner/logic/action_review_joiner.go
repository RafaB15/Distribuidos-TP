package negative_reviews_pre_filter

import (
	g "distribuidos-tp/internal/system_protocol/games"
	j "distribuidos-tp/internal/system_protocol/joiner"
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	p "distribuidos-tp/system/action_review_joiner/persistence"
	"github.com/op/go-logging"
)

const (
	AckBatchSize = 500

	EntryAmount      = 1
	GameMapperAmount = 1
)

type ReceiveMessageFunc func(messageTracker *n.MessageTracker) (clientID int, reviews []*r.RawReview, games []*g.Game, eof bool, newMessage bool, e error)
type SendReviewsFunc func(clientID int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, reviews []*r.Review, messageTracker *n.MessageTracker) error
type AckLastMessageFunc func() error
type SendEndOfFileFunc func(clientID int, senderID int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, messageTracker *n.MessageTracker) error

type ActionReviewJoiner struct {
	ReceiveMessage ReceiveMessageFunc
	SendReviews    SendReviewsFunc
	AckLastMessage AckLastMessageFunc
	SendEndOfFile  SendEndOfFileFunc
	logger         *logging.Logger
}

func NewActionReviewJoiner(
	receiveMessage ReceiveMessageFunc,
	sendReviews SendReviewsFunc,
	ackLastMessage AckLastMessageFunc,
	sendEndOfFile SendEndOfFileFunc,
	logger *logging.Logger,
) *ActionReviewJoiner {
	return &ActionReviewJoiner{
		ReceiveMessage: receiveMessage,
		SendReviews:    sendReviews,
		AckLastMessage: ackLastMessage,
		SendEndOfFile:  sendEndOfFile,
		logger:         logger,
	}
}

func (f *ActionReviewJoiner) Run(id int, repository *p.Repository, englishFiltersAmount int, actionReviewsAccumulatorsAmount int) {
	accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber, err := repository.LoadAll(EntryAmount + GameMapperAmount)
	if err != nil {
		f.logger.Errorf("Failed to load data: %v", err)
		return
	}

	messagesUntilAck := AckBatchSize

	for {
		clientID, reviews, games, eof, newMessage, err := f.ReceiveMessage(messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to receive message: %v", err)
			return
		}

		if newMessage && !eof {

			clientAccumulatedRawReviews, exists := accumulatedRawReviewsMap.Get(clientID)
			if !exists {
				clientAccumulatedRawReviews = repository.InitializeRawReviewMap()
				accumulatedRawReviewsMap.Set(clientID, clientAccumulatedRawReviews)
			}

			clientGamesToSend, exists := gamesToSendMap.Get(clientID)
			if !exists {
				clientGamesToSend = repository.InitializeGamesToSendMap()
				gamesToSendMap.Set(clientID, clientGamesToSend)
			}

			if reviews != nil {
				err := f.handleRawReviews(clientID, englishFiltersAmount, actionReviewsAccumulatorsAmount, clientAccumulatedRawReviews, clientGamesToSend, reviews, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle raw reviews: %v", err)
					return
				}
			}

			if games != nil {
				f.logger.Infof("Received games for client %d", clientID)
				err := f.handleGames(clientID, englishFiltersAmount, actionReviewsAccumulatorsAmount, clientAccumulatedRawReviews, clientGamesToSend, games, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle game reviews metrics: %v", err)
					return
				}
			}
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending EOFs")
			err = f.SendEndOfFile(clientID, id, englishFiltersAmount, actionReviewsAccumulatorsAmount, messageTracker)
			if err != nil {
				f.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			accumulatedRawReviewsMap.Delete(clientID)
			gamesToSendMap.Delete(clientID)
			messageTracker.DeleteClientInfo(clientID) //Sería mejor borrar toda la info

			syncNumber++
			err := repository.SaveAll(accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save data: %v", err)
				return
			}

			messagesUntilAck = AckBatchSize
			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}

		}

		if messagesUntilAck == 0 {
			syncNumber++
			err := repository.SaveAll(accumulatedRawReviewsMap, gamesToSendMap, messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save data: %v", err)
				return
			}

			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("error acking last message: %s", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}

func (f *ActionReviewJoiner) handleRawReviews(clientId int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[*j.GameToSend], rawReviews []*r.RawReview, messageTracker *n.MessageTracker) error {
	var reviewsToSend []*r.Review

	for _, rawReview := range rawReviews {
		if gameToSend, exists := clientGamesToSend.Get(int(rawReview.AppId)); exists {
			if gameToSend.ShouldSend && !rawReview.Positive {
				review := r.NewReview(rawReview.ReviewId, rawReview.AppId, gameToSend.Name, rawReview.Positive, rawReview.ReviewText)
				reviewsToSend = append(reviewsToSend, review)
			} else {
				continue
			}
		} else {
			if !rawReview.Positive {
				currentReviews, _ := clientAccumulatedRawReviews.Get(int(rawReview.AppId))
				clientAccumulatedRawReviews.Set(int(rawReview.AppId), append(currentReviews, rawReview))
			}
		}
	}

	if len(reviewsToSend) > 0 {
		err := f.SendReviews(clientId, englishFiltersAmount, actionReviewsAccumulatorsAmount, reviewsToSend, messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to send reviews: %v", err)
			return err
		}
	}

	return nil
}

func (f *ActionReviewJoiner) handleGames(clientId int, englishFiltersAmount int, actionReviewsAccumulatorsAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[*j.GameToSend], games []*g.Game, messageTracker *n.MessageTracker) error {
	var reviewsToSend []*r.Review

	for _, game := range games {
		gameToSend := j.NewGameToSend(game.AppId, game.Name, game.Action)
		clientGamesToSend.Set(int(game.AppId), gameToSend)
		if gameToSend.ShouldSend {
			if reviews, exists := clientAccumulatedRawReviews.Get(int(gameToSend.AppId)); exists {
				for _, rawReview := range reviews {
					review := r.NewReview(rawReview.ReviewId, rawReview.AppId, gameToSend.Name, rawReview.Positive, rawReview.ReviewText)
					reviewsToSend = append(reviewsToSend, review)
				}
				clientAccumulatedRawReviews.Delete(int(gameToSend.AppId))
			}
		} else {
			clientAccumulatedRawReviews.Delete(int(gameToSend.AppId))
		}
	}

	err := f.SendReviews(clientId, englishFiltersAmount, actionReviewsAccumulatorsAmount, reviewsToSend, messageTracker)
	if err != nil {
		f.logger.Errorf("Failed to send reviews: %v", err)
		return err
	}

	return nil
}
