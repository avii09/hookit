package input

import (
	"encoding/csv"
	"os"
)

// ReadCSV reads the data from a CSV file.
func ReadCSV(filePath string) ([]map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read() // Read the header row
	if err != nil {
		return nil, err
	}

	var rows []map[string]string
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		row := make(map[string]string)
		for i, header := range headers {
			row[header] = record[i]
		}
		rows = append(rows, row)
	}

	return rows, nil
}
