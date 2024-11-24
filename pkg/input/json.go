package input

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

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

// ConvertMapToStringMap converts a slice of maps with interface{} values to a slice of maps with string values.
func ConvertMapToStringMap(data []map[string]interface{}) ([]map[string]string, error) {
    var stringData []map[string]string
    // Iterate over each map (row) in the data slice.
    for _, row := range data {
        stringRow := make(map[string]string)
        // Convert each key-value pair in the map to string.
        for key, value := range row {
            switch v := value.(type) {
            case string:
                stringRow[key] = v
            case float64:
                // Format float values to avoid excessive decimal places.
                stringRow[key] = strconv.FormatFloat(v, 'f', -1, 64)
            case int, int64:
                // Convert integers to string.
                stringRow[key] = fmt.Sprintf("%d", v)
            case bool:
                // Convert boolean values to "true"/"false".
                stringRow[key] = strconv.FormatBool(v)
            case nil:
                // Handle nil values explicitly as an empty string.
                stringRow[key] = ""
            default:
                // Return an error for unsupported types.
                return nil, fmt.Errorf("unsupported value type for key '%s': %T", key, v)
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
