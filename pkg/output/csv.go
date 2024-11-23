package output

import (
	"encoding/csv"
	"os"
)

// WriteCSV writes the data to a CSV file.
func WriteCSV(filePath string, data []map[string]string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header row
	if len(data) > 0 {
		headers := []string{}
		for key := range data[0] {
			headers = append(headers, key)
		}
		writer.Write(headers)

		// Write rows
		for _, row := range data {
			record := make([]string, len(headers))
			for i, header := range headers {
				record[i] = row[header]
			}
			writer.Write(record)
		}
	}

	return nil
}
