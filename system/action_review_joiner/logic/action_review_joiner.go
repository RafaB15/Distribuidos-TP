package negative_reviews_pre_filter

import (
	g "distribuidos-tp/internal/system_protocol/games"
	n "distribuidos-tp/internal/system_protocol/node"
	p "distribuidos-tp/system/action_review_joiner/persistence"

	r "distribuidos-tp/internal/system_protocol/reviews"
	"github.com/op/go-logging"
)

const (
	AckBatchSize = 500

	EntryAmount      = 1
	GameMapperAmount = 1
)

type ReceiveMessageFunc func(messageTracker *n.MessageTracker) (clientID int, reviews []*r.RawReview, games []*g.Game, eof bool, newMessage bool, e error)
type SendReviewFunc func(clientID int, englishFiltersAmount int, review *r.RawReview, tracker *n.MessageTracker) error
type AckLastMessageFunc func() error
type SendEndOfFileFunc func(clientID int, senderID int, englishFiltersAmount int, tracker *n.MessageTracker) error

type ActionReviewJoiner struct {
	ReceiveMessage ReceiveMessageFunc
	SendReview     SendReviewFunc
	AckLastMessage AckLastMessageFunc
	SendEndOfFile  SendEndOfFileFunc
	logger         *logging.Logger
}

func NewActionReviewJoiner(
	receiveMessage ReceiveMessageFunc,
	sendReview SendReviewFunc,
	ackLastMessage AckLastMessageFunc,
	sendEndOfFile SendEndOfFileFunc,
	logger *logging.Logger,
) *ActionReviewJoiner {
	return &ActionReviewJoiner{
		ReceiveMessage: receiveMessage,
		SendReview:     sendReview,
		AckLastMessage: ackLastMessage,
		SendEndOfFile:  sendEndOfFile,
		logger:         logger,
	}
}

func (f *ActionReviewJoiner) Run(id int, repository *p.Repository, englishFiltersAmount int) {
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
				err := f.handleRawReviews(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, reviews, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle raw reviews: %v", err)
					return
				}
			}

			if games != nil {
				f.logger.Infof("Received games for client %d", clientID)
				err := f.handleGames(clientID, englishFiltersAmount, clientAccumulatedRawReviews, clientGamesToSend, games, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to handle game reviews metrics: %v", err)
					return
				}
			}
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending EOFs")
			err = f.SendEndOfFile(clientID, id, englishFiltersAmount, messageTracker)
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

func (f *ActionReviewJoiner) handleRawReviews(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], rawReviews []*r.RawReview, messageTracker *n.MessageTracker) error {
	for _, rawReview := range rawReviews {
		if shouldSend, exists := clientGamesToSend.Get(int(rawReview.AppId)); exists {
			if shouldSend && !rawReview.Positive {
				err := f.SendReview(clientId, englishFiltersAmount, rawReview, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to send review: %v", err)
					return err
				}
				f.logger.Infof("Sent review for client %d", clientId)
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
	return nil
}

func (f *ActionReviewJoiner) handleGames(clientId int, englishFiltersAmount int, clientAccumulatedRawReviews *n.IntMap[[]*r.RawReview], clientGamesToSend *n.IntMap[bool], games []*g.Game, messageTracker *n.MessageTracker) error {
	for _, game := range games {
		if game.Action {
			clientGamesToSend.Set(int(game.AppId), true)
			if reviews, exists := clientAccumulatedRawReviews.Get(int(game.AppId)); exists {
				// Ver de mandar batches
				for _, rawReview := range reviews {
					err := f.SendReview(clientId, englishFiltersAmount, rawReview, messageTracker)
					if err != nil {
						f.logger.Errorf("Failed to send review: %v", err)
						return err
					}
				}
				clientAccumulatedRawReviews.Delete(int(game.AppId))
			}
		} else {
			clientGamesToSend.Set(int(game.AppId), false)
			clientAccumulatedRawReviews.Delete(int(game.AppId))
		}
	}

	return nil
}
