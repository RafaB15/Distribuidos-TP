package final_negative_joiner

import (
	j "distribuidos-tp/internal/system_protocol/joiner"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type FinalNegativeJoiner struct {
	ReceiveJoinedGameReviews func() (int, *j.JoinedNegativeGameReview, bool, error)
	SendMetrics              func(int, []*j.JoinedNegativeGameReview) error
	SendEof                  func(int) error
}

func NewFinalNegativeJoiner(receiveJoinedGameReviews func() (int, *j.JoinedNegativeGameReview, bool, error), sendMetrics func(int, []*j.JoinedNegativeGameReview) error, sendEof func(int) error) *FinalNegativeJoiner {
	return &FinalNegativeJoiner{
		ReceiveJoinedGameReviews: receiveJoinedGameReviews,
		SendMetrics:              sendMetrics,
		SendEof:                  sendEof,
	}
}

func (f *FinalNegativeJoiner) Run(actionNegativeJoinersAmount int) {
	remainingEOFsMap := make(map[int]int)
	accumulatedGameReviews := make(map[int][]*j.JoinedNegativeGameReview)

	for {
		clientID, joinedGamesReviews, eof, err := f.ReceiveJoinedGameReviews()
		if err != nil {
			log.Errorf("Failed to receive message: %v", err)
			return
		}

		clientAccumulatedGameReviews, exists := accumulatedGameReviews[clientID]
		if !exists {
			clientAccumulatedGameReviews = []*j.JoinedNegativeGameReview{}
			accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		}

		if eof {
			log.Infof("Received EOF for client %d", clientID)

			remainingEOFs, exists := remainingEOFsMap[clientID]
			if !exists {
				remainingEOFs = actionNegativeJoinersAmount
			}

			remainingEOFs--
			remainingEOFsMap[clientID] = remainingEOFs
			if remainingEOFs > 0 {
				continue
			}

			log.Infof("Received all EOFs of client: %d, sending EOF to writer")
			err = f.SendMetrics(clientID, clientAccumulatedGameReviews)
			if err != nil {
				log.Errorf("Failed to send metrics: %v", err)
				return
			}
			log.Infof("Sent all negative joined game reviews from client %d to client", clientID)
			delete(accumulatedGameReviews, clientID)
			delete(remainingEOFsMap, clientID)
			continue
		}

		log.Infof("Received joined negative game reviews from client %d", clientID)
		clientAccumulatedGameReviews = append(clientAccumulatedGameReviews, joinedGamesReviews)
		accumulatedGameReviews[clientID] = clientAccumulatedGameReviews
		log.Infof("Saved joined negative game reviews from client %d", clientID)
	}
}
