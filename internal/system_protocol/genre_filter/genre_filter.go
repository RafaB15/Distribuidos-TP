package genre_filter

import "strings"

func BelongsToGenre(genre string, record []string) bool {
	genres := strings.Split(record[5], ",")

	for _, g := range genres {
		if g == genre {
			return true
		}
	}
	return false

}
