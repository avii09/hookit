package output

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// WriteJSON writes the transformed data to a JSON output file
func WriteJSON(filePath string, data []map[string]string) error {
	// Marshal the data into JSON format
	dataBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling data to JSON: %v", err)
	}

	// Write the JSON data to the output file
	err = ioutil.WriteFile(filePath, dataBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing JSON to file: %v", err)
	}

	return nil
}

// ProcessJSONOutput processes the transformed data and writes it to a JSON file
func ProcessJSONOutput(filePath string, data []map[string]string) error {
	// Write the data to JSON
	err := WriteJSON(filePath, data)
	if err != nil {
		return fmt.Errorf("error writing JSON output: %v", err)
	}

	return nil
}
