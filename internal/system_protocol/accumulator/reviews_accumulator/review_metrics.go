package os_accumulator

type ReviewMetrics struct {
	AppID           int
	PositiveReviews int
	NegativeReviews int
}

func NewReviewsMetrics(appId int) *ReviewMetrics {

	return &ReviewMetrics{
		AppID:           appId,
		PositiveReviews: 0,
		NegativeReviews: 0,
	}
}

//tambien podriamos/deberiamos tener un merge para poder acumular las metricas de las que ya estamos llevando la cuenta
