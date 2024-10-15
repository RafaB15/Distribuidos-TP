package main

import u "distribuidos-tp/internal/utils"

const (
	OSAcumulatorsAmountEnvironmentVariableName       = "OS_ACCUMULATORS_AMOUNT"
	DecadeFilterAmountEnvironmentVariableName        = "DECADE_FILTER_AMOUNT"
	IndieReviewJoinersAmountEnvironmentVariableName  = "INDIE_REVIEW_JOINERS_AMOUNT"
	ActionReviewJoinersAmountEnvironmentVariableName = "ACTION_REVIEW_JOINERS_AMOUNT"
)

func main() {
	osAccumulatorsAmount, err := u.GetEnvInt(OSAcumulatorsAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	decadeFilterAmount, err := u.GetEnvInt(DecadeFilterAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	indieReviewJoinersAmount, err := u.GetEnvInt(IndieReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}

	actionReviewJoinersAmount, err := u.GetEnvInt(ActionReviewJoinersAmountEnvironmentVariableName)
	if err != nil {
		log.Errorf("Failed to get environment variable: %v", err)
		return
	}
}
