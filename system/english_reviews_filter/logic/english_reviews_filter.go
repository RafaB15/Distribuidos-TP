package english_reviews_filter

import (
	n "distribuidos-tp/internal/system_protocol/node"
	r "distribuidos-tp/internal/system_protocol/reviews"
	"github.com/op/go-logging"
)

const (
	AckBatchSize = 100
)

type ReceiveGameReviewsFunc func(messageTracker *n.MessageTracker) (clientID int, review *r.Review, eof bool, newMessage bool, e error)
type SendEnglishReviewFunc func(clientID int, reducedReview *r.ReducedReview, englishAccumulatorsAmount int) error
type SendEndOfFilesFunc func(clientID int, accumulatorsAmount int) error
type AckLastMessageFunc func() error

type EnglishReviewsFilter struct {
	ReceiveGameReviews ReceiveGameReviewsFunc
	SendEnglishReview  SendEnglishReviewFunc
	SendEnfOfFiles     SendEndOfFilesFunc
	AckLastMessage     AckLastMessageFunc
	logger             *logging.Logger
}

func NewEnglishReviewsFilter(
	receiveGameReviews ReceiveGameReviewsFunc,
	sendEnglishReviews SendEnglishReviewFunc,
	sendEndOfFiles SendEndOfFilesFunc,
	ackLastMessage AckLastMessageFunc,
	logger *logging.Logger,
) *EnglishReviewsFilter {
	return &EnglishReviewsFilter{
		ReceiveGameReviews: receiveGameReviews,
		SendEnglishReview:  sendEnglishReviews,
		SendEnfOfFiles:     sendEndOfFiles,
		AckLastMessage:     ackLastMessage,
		logger:             logger,
	}
}

func (f *EnglishReviewsFilter) Run(accumulatorsAmount int, negativeReviewsPreFilterAmount int) {
	messageTracker := n.NewMessageTracker(negativeReviewsPreFilterAmount)

	messagesUntilAck := AckBatchSize

	languageIdentifier := r.NewLanguageIdentifier()

	for {
		clientID, review, eof, newMessage, err := f.ReceiveGameReviews(messageTracker)
		if err != nil {
			f.logger.Errorf("Failed to receive game review: %v", err)
			return
		}

		if newMessage && !eof {
			if languageIdentifier.IsEnglish(review.ReviewText) {
				review := r.NewReducedReview(review.ReviewId, review.AppId, review.Name, review.Positive)
				err := f.SendEnglishReview(clientID, review, accumulatorsAmount)
				if err != nil {
					f.logger.Errorf("Failed to send english review: %v", err)
					return
				}
				f.logger.Infof("Sent english review for app %d", review.AppId)
			} else {
				f.logger.Infof("ReducedReview for app %d is not in english", review.AppId)
			}
		}

		if messageTracker.ClientFinished(clientID, f.logger) {
			f.logger.Infof("Client %d finished", clientID)

			f.logger.Info("Sending EOFs")
			err = f.SendEnfOfFiles(clientID, accumulatorsAmount)
			if err != nil {
				f.logger.Errorf("Failed to send EOF: %v", err)
				return
			}

			messageTracker.DeleteClientInfo(clientID)

			messagesUntilAck = AckBatchSize
			err = f.AckLastMessage()
			if err != nil {
				f.logger.Errorf("Failed to ack last message: %v", err)
				return
			}
		}

		if messagesUntilAck == 0 {
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
