package decade_filter

import (
	df "distribuidos-tp/internal/system_protocol/decade_filter"

	"github.com/op/go-logging"
)

const DECADE = 2010

var log = logging.MustGetLogger("log")

type DecadeFilter struct {
	ReceiveYearAvgPtf      func() ([]*df.GameYearAndAvgPtf, bool, error)
	SendFilteredYearAvgPtf func([]*df.GameYearAndAvgPtf) error
	SendEof                func() error
}

func NewDecadeFilter(receiveYearAvgPtf func() ([]*df.GameYearAndAvgPtf, bool, error), sendFilteredYearAvgPtf func([]*df.GameYearAndAvgPtf) error, sendEof func() error) *DecadeFilter {
	return &DecadeFilter{
		ReceiveYearAvgPtf:      receiveYearAvgPtf,
		SendFilteredYearAvgPtf: sendFilteredYearAvgPtf,
		SendEof:                sendEof,
	}
}

func (d *DecadeFilter) Run() {
	for {

		yearAvgPtfSlice, eof, err := d.ReceiveYearAvgPtf()

		if err != nil {
			log.Errorf("failed to receive year and avg ptf: %v", err)
			return
		}

		if eof {
			log.Infof("received EOF. Sending filtered year and avg ptf")
			err = d.SendEof()
			if err != nil {
				log.Errorf("failed to send EOF: %v", err)
				return
			}
			continue
		}

		yearsAvgPtfFiltered := df.FilterByDecade(yearAvgPtfSlice, DECADE)

		err = d.SendFilteredYearAvgPtf(yearsAvgPtfFiltered)

		if err != nil {
			log.Errorf("failed to send filtered year and avg ptf: %v", err)
			return
		}

	}
}
