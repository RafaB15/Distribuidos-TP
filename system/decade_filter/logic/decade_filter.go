package decade_filter

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

const DECADE = 2010

var log = logging.MustGetLogger("log")

type DecadeFilter struct {
	ReceiveYearAvgPtf      func() (int, []*df.GameYearAndAvgPtf, bool, error)
	SendFilteredYearAvgPtf func(int, []*df.GameYearAndAvgPtf) error
	SendEof                func(int) error
}

func NewDecadeFilter(receiveYearAvgPtf func() (int, []*df.GameYearAndAvgPtf, bool, error), sendFilteredYearAvgPtf func(int, []*df.GameYearAndAvgPtf) error, sendEof func(int) error) *DecadeFilter {
	return &DecadeFilter{
		ReceiveYearAvgPtf:      receiveYearAvgPtf,
		SendFilteredYearAvgPtf: sendFilteredYearAvgPtf,
		SendEof:                sendEof,
	}
}

func (d *DecadeFilter) Run() {
	for {

		clientID, yearAvgPtfSlice, eof, err := d.ReceiveYearAvgPtf()

		if err != nil {
			log.Errorf("failed to receive year and avg ptf: %v", err)
			return
		}

		if eof {
			log.Infof("Received client %d EOF. Sending EOF to top ten accumulator", clientID)
			err = d.SendEof(clientID)
			if err != nil {
				log.Errorf("failed to send EOF: %v", err)
				return
			}
			continue
		}

		yearsAvgPtfFiltered := df.FilterByDecade(yearAvgPtfSlice, DECADE)

		log.Infof("Sending ClientID %d filtered year and avg ptf to top ten accumulator", clientID)
		err = d.SendFilteredYearAvgPtf(clientID, yearsAvgPtfFiltered)

		if err != nil {
			log.Errorf("failed to send filtered year and avg ptf: %v", err)
			return
		}

	}
}
