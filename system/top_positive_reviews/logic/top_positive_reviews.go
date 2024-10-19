package top_positive_reviews

import (
	j "distribuidos-tp/internal/system_protocol/joiner"
	"sort"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type TopPositiveReviews struct {
	ReceiveMsg  func() (*j.JoinedActionGameReview, bool, error)
	SendMetrics func([]*j.JoinedActionGameReview) error
	SendEof     func() error
}

func NewTopPositiveReviews(receiveMsg func() (*j.JoinedActionGameReview, bool, error), sendMetrics func([]*j.JoinedActionGameReview) error, sendEof func() error) *TopPositiveReviews {
	return &TopPositiveReviews{
		ReceiveMsg:  receiveMsg,
		SendMetrics: sendMetrics,
		SendEof:     sendEof,
	}
}

func (t *TopPositiveReviews) Run(indieReviewJoinerAmount int) {
	topPositiveIndieGames := make([]*j.JoinedActionGameReview, 0)
	remainingEOFs := indieReviewJoinerAmount

	for {
		msg, eof, err := t.ReceiveMsg()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		if eof {
			log.Infof("Received EOF")
			remainingEOFs--
			if remainingEOFs > 0 {
				continue
			}
			err = t.SendMetrics(topPositiveIndieGames)
			if err != nil {
				log.Errorf("Failed to send metrics: %v", err)
				return
			}
			log.Infof("Sent Top 5 positive reviews to writer")
			err = t.SendEof()
			if err != nil {
				log.Errorf("Failed to send EOF: %v", err)
				return
			}
			log.Info("Sent eof to writer")
			continue
		}

		log.Infof("Received indie game with ID: %v", msg.AppId)
		log.Infof("Evaluating number of positive reviews and saving game")
		topPositiveIndieGames = append(topPositiveIndieGames, msg)
		if len(topPositiveIndieGames) > 5 {
			// Sort the slice by positive reviews in descending order
			sort.Slice(topPositiveIndieGames, func(i, j int) bool {
				return topPositiveIndieGames[i].PositiveReviews > topPositiveIndieGames[j].PositiveReviews
			})
			// Keep only the top 5
			topPositiveIndieGames = topPositiveIndieGames[:5]
		}

	}

}
