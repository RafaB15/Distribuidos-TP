package compare_results

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Función para leer y agrupar las líneas por query
func readAndGroupByQuery(filename string) (map[string][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	queryMap := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	var currentQuery string
	queryPattern := regexp.MustCompile(`^Query \d+:`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if queryPattern.MatchString(line) {
			currentQuery = line
			if _, exists := queryMap[currentQuery]; !exists {
				queryMap[currentQuery] = []string{}
			}
		} else if currentQuery != "" {
			queryMap[currentQuery] = append(queryMap[currentQuery], line)
		}
	}

	for query := range queryMap {
		sort.Strings(queryMap[query])
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queryMap, nil
}

// Función para comparar las queries entre los archivos
func compareQueries(file1, file2 string) error {
	queriesFile1, err := readAndGroupByQuery(file1)
	if err != nil {
		return err
	}
	queriesFile2, err := readAndGroupByQuery(file2)
	if err != nil {
		return err
	}

	allQueries := make(map[string]bool)
	for query := range queriesFile1 {
		allQueries[query] = true
	}
	for query := range queriesFile2 {
		allQueries[query] = true
	}

	sortedQueries := make([]string, 0, len(allQueries))
	for query := range allQueries {
		sortedQueries = append(sortedQueries, query)
	}
	sort.Strings(sortedQueries)

	for _, query := range sortedQueries {
		linesFile1 := queriesFile1[query]
		linesFile2 := queriesFile2[query]

		setFile1 := make(map[string]bool)
		for _, line := range linesFile1 {
			setFile1[line] = true
		}

		setFile2 := make(map[string]bool)
		for _, line := range linesFile2 {
			setFile2[line] = true
		}

		fmt.Printf("\nDiferencias en %s:\n", query)

		foundDifferences := false

		for line := range setFile1 {
			if !setFile2[line] {
				fmt.Printf("  Extra en %s: %s\n", file1, line)
				foundDifferences = true
			}
		}

		for line := range setFile2 {
			if !setFile1[line] {
				fmt.Printf("  Extra en %s: %s\n", file2, line)
				foundDifferences = true
			}
		}

		if !foundDifferences {
			fmt.Println("  No hay diferencias.")
		}
	}

	return nil
}

// CompareResults is a wrapper to call compareQueries from other packages
func CompareResults(file1, file2 string) error {
	return compareQueries(file1, file2)
}
