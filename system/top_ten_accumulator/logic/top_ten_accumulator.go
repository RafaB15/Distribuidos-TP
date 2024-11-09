package top_ten_accumulator

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"
	rp "distribuidos-tp/system/top_ten_accumulator/persistence"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type TopTenAccumulator struct {
	ReceiveMsg func() (int, []*df.GameYearAndAvgPtf, bool, error)
	SendMsg    func(int, []*df.GameYearAndAvgPtf) error
	Repository *rp.Repository
}

func NewTopTenAccumulator(receiveMsg func() (int, []*df.GameYearAndAvgPtf, bool, error), sendMsg func(int, []*df.GameYearAndAvgPtf) error) *TopTenAccumulator {
	return &TopTenAccumulator{
		ReceiveMsg: receiveMsg,
		SendMsg:    sendMsg,
		Repository: rp.NewRepository("top_ten_accumulator"),
	}
}

func (t *TopTenAccumulator) Run(decadeFilterAmount int, fileName string) {

	for {

		clientID, decadeGames, eof, err := t.ReceiveMsg()
		if err != nil {
			log.Errorf("failed to receive message: %v", err)
			return
		}

		clientTopTenGames, err := t.Repository.Load(clientID)
		if err != nil {
			log.Errorf("failed to load client %d top ten games: %v", clientID, err)
			continue
		}

		log.Infof("Received decade games")

		if eof {
			remainingEOFs, err := t.Repository.PersistAndUpdateEof(clientID, decadeFilterAmount)
			if err != nil {
				fmt.Printf("failed to persist and update eof: %v\n", err)
				continue
			}

			if remainingEOFs <= 0 {
				log.Infof("Received all EOFs of client %d, sending final top ten games", clientID)

				err = t.SendMsg(clientID, clientTopTenGames)
				if err != nil {
					log.Errorf("failed to send metrics: %v", err)
					return
				}
			}
		}

		clientTopTenGames = df.TopTenAvgPlaytimeForever(append(clientTopTenGames, decadeGames...))
		t.Repository.Persist(clientID, clientTopTenGames)
		log.Infof("Updated top ten games for client %d", clientID)

	}
}
