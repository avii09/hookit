package output

import (
	"encoding/csv"
	"os"
)

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
	}

	// Write rows
	for _, row := range data {
		record := []string{}
		for _, value := range row {
			record = append(record, value)
		}
		writer.Write(record)
	}

	return nil
}
