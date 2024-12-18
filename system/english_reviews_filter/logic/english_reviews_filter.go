package english_reviews_filter

import (
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	p "distribuidos-tp/system/english_reviews_filter/persistence"
	"github.com/op/go-logging"
)

const (
	AckBatchSize = 250
)

type ReceiveGameReviewsFunc func(messageTracker *n.MessageTracker) (clientID int, review *r.Review, eof bool, newMessage bool, delMessage bool, e error)
type SendEnglishReviewFunc func(clientID int, reducedReview *r.ReducedReview, englishAccumulatorsAmount int, messageTracker *n.MessageTracker) error
type SendEndOfFilesFunc func(clientID int, senderID int, accumulatorsAmount int, messageTracker *n.MessageTracker) error
type SendDeleteClientFunc func(clientID int, accumulatorsAmount int) error
type AckLastMessageFunc func() error

type EnglishReviewsFilter struct {
	ReceiveGameReviews ReceiveGameReviewsFunc
	SendEnglishReview  SendEnglishReviewFunc
	SendEnfOfFiles     SendEndOfFilesFunc
	SendDeleteClient   SendDeleteClientFunc
	AckLastMessage     AckLastMessageFunc
	logger             *logging.Logger
}

func NewEnglishReviewsFilter(
	receiveGameReviews ReceiveGameReviewsFunc,
	sendEnglishReviews SendEnglishReviewFunc,
	sendEndOfFiles SendEndOfFilesFunc,
	sendDeleteClient SendDeleteClientFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *EnglishReviewsFilter {
	return &EnglishReviewsFilter{
		ReceiveGameReviews: receiveGameReviews,
		SendEnglishReview:  sendEnglishReviews,
		SendEnfOfFiles:     sendEndOfFiles,
		SendDeleteClient:   sendDeleteClient,
		AckLastMessage:     ackLastMessage,
		logger:             logger,
	}
}

func (f *EnglishReviewsFilter) Run(id int, accumulatorsAmount int, actionReviewJoinersAmount int, repository *p.Repository) {
	messageTracker, syncNumber := repository.LoadMessageTracker(actionReviewJoinersAmount)

	messagesUntilAck := AckBatchSize

	languageIdentifier := r.NewLanguageIdentifier()

	for {
		clientID, review, eof, newMessage, delMessage, err := f.ReceiveGameReviews(messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to receive game review: %v", err)
			return
		}

		if newMessage && !eof && !delMessage {
			if languageIdentifier.IsEnglish(review.ReviewText) {
				review := r.NewReducedReview(review.ReviewId, review.AppId, review.Name, review.Positive)
				err := f.SendEnglishReview(clientID, review, accumulatorsAmount, messageTracker)
				if err != nil {
					f.logger.Errorf("Failed to send english review: %v", err)
					return
				}
			}
		}

		if delMessage {
			f.logger.Infof("Deleting client %d", clientID)
			err := f.SendDeleteClient(clientID, accumulatorsAmount)
			if err != nil {
				f.logger.Errorf("Failed to delete client: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
		}

		clientFinished := messageTracker.ClientFinished(clientID, f.logger)
		if clientFinished {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending EOFs")
			err = f.SendEnfOfFiles(clientID, id, accumulatorsAmount, messageTracker)
			if err != nil {
				f.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)
		}

		if messagesUntilAck == 0 || delMessage || clientFinished {
			syncNumber++
			err = repository.SaveMessageTracker(messageTracker, syncNumber)
			if err != nil {
				f.logger.Errorf("Failed to save message tracker: %v", err)
				return
			}

			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
			messagesUntilAck = AckBatchSize
		} else {
			messagesUntilAck--
		}
	}
}
