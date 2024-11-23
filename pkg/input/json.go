package input

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/avii09/hookit/pkg/transform"
)

// ReadJSON reads the input JSON file and returns the data as a slice of maps.
func ReadJSON(filePath string) ([]map[string]interface{}, error) {
	// Open the JSON file.
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening JSON file: %v", err)
	}
	defer file.Close()

	// Read the file content.
	dataBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading JSON file: %v", err)
	}

	// Parse the JSON data.
	var data []map[string]interface{}
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %v", err)
	}

	return data, nil
}

// ConvertMapToStringMap converts a map with interface{} values to a map with string values.
func ConvertMapToStringMap(data []map[string]interface{}) ([]map[string]string, error) {
	var stringData []map[string]string

	// Iterate over each row in the data.
	for _, row := range data {
		stringRow := make(map[string]string)

		// Convert each value in the row to a string.
		for key, value := range row {
			switch v := value.(type) {
			case string:
				stringRow[key] = v
			case float64: // JSON numbers are parsed as float64.
				stringRow[key] = fmt.Sprintf("%f", v)
			default:
				// Return an error if the type is unsupported.
				return nil, fmt.Errorf("unsupported value type: %T", v)
			}
		}

		stringData = append(stringData, stringRow)
	}

	return stringData, nil
}

// ProcessJSONInput processes the input JSON based on transformations.
func ProcessJSONInput(filePath string, rules transform.TransformationRules) ([]map[string]string, error) {
	// Read JSON data from the input file.
	data, err := ReadJSON(filePath)
	if err != nil {
		return nil, err
	}

	// Convert data to map[string]string.
	stringData, err := ConvertMapToStringMap(data)
	if err != nil {
		return nil, err
	}

	// Apply transformations to the data.
	transformedData := transform.ApplyTransformations(stringData, rules)

	return transformedData, nil
}
