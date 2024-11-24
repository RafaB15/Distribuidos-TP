package reviews_accumulator

import (
	r "distribuidos-tp/internal/system_protocol/reviews"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
)

type GameReviewsMetrics struct {
	AppID           uint32
	PositiveReviews int
	NegativeReviews int
}

func NewReviewsMetrics(appId uint32) *GameReviewsMetrics {

	return &GameReviewsMetrics{
		AppID:           appId,
		PositiveReviews: 0,
		NegativeReviews: 0,
	}
}

func (m *GameReviewsMetrics) UpdateWithReview(review *r.ReducedReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func (m *GameReviewsMetrics) UpdateWithRawReview(review *r.RawReview) {
	if review.Positive {
		m.PositiveReviews += 1
	} else {
		m.NegativeReviews += 1
	}
}

func SerializeGameReviewsMetrics(metrics *GameReviewsMetrics) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], metrics.AppID)
	binary.BigEndian.PutUint32(buf[4:8], uint32(metrics.PositiveReviews))
	binary.BigEndian.PutUint32(buf[8:12], uint32(metrics.NegativeReviews))
	return buf
}

func DeserializeGameReviewsMetrics(data []byte) (*GameReviewsMetrics, error) {
	if len(data) != 12 {
		return nil, errors.New("Data too short to deserialize into GameReviewsMetrics")
	}

	metrics := &GameReviewsMetrics{
		AppID:           binary.BigEndian.Uint32(data[0:4]),
		PositiveReviews: int(binary.BigEndian.Uint32(data[4:8])),
		NegativeReviews: int(binary.BigEndian.Uint32(data[8:12])),
	}
	return metrics, nil
}

func ReadGameReviewsMetricsFromFile(filename string) ([]*GameReviewsMetrics, error) {
	fileData, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// Si el archivo no existe, devuelve una lista vacía
			return []*GameReviewsMetrics{}, nil
		}
		return nil, err
	}

	var games []*GameReviewsMetrics
	for i := 0; i < len(fileData); i += 12 {
		game, err := DeserializeGameReviewsMetrics(fileData[i : i+12])
		if err != nil {
			return nil, err
		}
		games = append(games, game)
	}
	return games, nil
}

// Escribe una lista de juegos al archivo
func WriteGameReviewsMetricsToFile(filename string, games []*GameReviewsMetrics) error {
	var data []byte
	for _, game := range games {
		data = append(data, SerializeGameReviewsMetrics(game)...)
	}
	return os.WriteFile(filename, data, 0644)
}

func AddGamesAndMaintainOrder(filename string, newGames []*GameReviewsMetrics) error {
	// Leer los juegos existentes del archivo
	existingGames, err := ReadGameReviewsMetricsFromFile(filename)
	if err != nil {
		return err
	}

	// Combinar la lista existente con la nueva lista
	allGames := append(existingGames, newGames...)

	// Ordenar los juegos por la cantidad de reseñas negativas
	sort.Slice(allGames, func(i, j int) bool {
		return allGames[i].NegativeReviews < allGames[j].NegativeReviews
	})

	// Escribir la lista ordenada de vuelta al archivo
	return WriteGameReviewsMetricsToFile(filename, allGames)
}

func AddGamesAndMaintainOrderV2(existingGames []*GameReviewsMetrics, newGames []*GameReviewsMetrics) []*GameReviewsMetrics {
	// Filter out games with zero negative reviews
	filteredNewGames := make([]*GameReviewsMetrics, 0)
	for _, game := range newGames {
		if game.NegativeReviews > 0 {
			filteredNewGames = append(filteredNewGames, game)
		}
	}

	allGames := append(existingGames, filteredNewGames...)

	sort.Slice(allGames, func(i, j int) bool {
		return allGames[i].NegativeReviews < allGames[j].NegativeReviews
	})

	return allGames
}

// Función para hacer un merge de dos listas ordenadas
func MergeSortedGames(existingGames, newGames []*GameReviewsMetrics) []*GameReviewsMetrics {
	result := make([]*GameReviewsMetrics, 0, len(existingGames)+len(newGames))
	i, j := 0, 0

	// Hacer merge de las dos listas ya ordenadas
	for i < len(existingGames) && j < len(newGames) {
		if existingGames[i].NegativeReviews <= newGames[j].NegativeReviews {
			result = append(result, existingGames[i])
			i++
		} else {
			result = append(result, newGames[j])
			j++
		}
	}

	// Agregar el resto de elementos si alguno quedó
	for i < len(existingGames) {
		result = append(result, existingGames[i])
		i++
	}

	for j < len(newGames) {
		result = append(result, newGames[j])
		j++
	}

	return result
}

func AddSortedGamesAndMaintainOrder(filename string, newGames []*GameReviewsMetrics) error {
	// Leer los juegos existentes del archivo
	existingGames, err := ReadGameReviewsMetricsFromFile(filename)
	if err != nil {
		return err
	}

	// Combinar las dos listas usando merge sort
	mergedGames := MergeSortedGames(existingGames, newGames)

	// Escribir la lista combinada y ordenada de vuelta al archivo
	return WriteGameReviewsMetricsToFile(filename, mergedGames)
}

// Retorna los juegos por encima del percentil 90 en reseñas negativas
func GetTop10PercentByNegativeReviews(filename string) ([]*GameReviewsMetrics, error) {
	// Leer todos los juegos ordenados del archivo
	games, err := ReadGameReviewsMetricsFromFile(filename)
	if err != nil {
		return nil, err
	}

	// Log the length of the games slice
	fmt.Printf("Total number of games: %d\n", len(games))

	// Si no hay juegos, devolver error
	if len(games) == 0 {
		return nil, errors.New("no games found in file")
	}

	// Calcular la posición del percentil 90
	percentileIndex := int(math.Ceil(0.9 * float64(len(games))))

	fmt.Printf("Reviews must have more than %d negative reviews to be considered\n", games[percentileIndex].NegativeReviews)

	// Retornar solo los juegos que están por encima del percentil 90
	return games[percentileIndex:], nil
}

func GetTop10PercentByNegativeReviewsV2(games []*GameReviewsMetrics) ([]*GameReviewsMetrics, error) {
	// Log the length of the games slice
	fmt.Printf("Total number of games: %d\n", len(games))

	// Si no hay juegos, devolver error
	if len(games) == 0 {
		return nil, errors.New("no games found in file")
	}

	// Calcular la posición del percentil 90
	percentileIndex := int(math.Floor(0.9 * float64(len(games))))

	fmt.Printf("Reviews must have more than %d negative reviews to be considered\n", games[percentileIndex].NegativeReviews)

	// Retornar solo los juegos que están por encima del percentil 90

	// Poner en una lista los juegos que están por encima del percentil 90
	// y retornarla
	overPercentile := make([]*GameReviewsMetrics, 0)
	for _, game := range games {
		if game.NegativeReviews >= games[percentileIndex].NegativeReviews {
			overPercentile = append(overPercentile, game)
		}
	}

	return overPercentile, nil
}
