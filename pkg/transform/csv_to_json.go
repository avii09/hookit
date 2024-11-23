package transform

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
)

// Define transformation rules structures for CSV
type CSVTransformationRules struct {
	Filter      []CSVFilterRule      `yaml:"filter"`
	Mapping     CSVMappingRules      `yaml:"mapping"`
	Aggregation []CSVAggregationRule `yaml:"aggregation"`
}

type CSVFilterRule struct {
	Column    string `yaml:"column"`
	Condition string `yaml:"condition"`
}

type CSVMappingRules struct {
	DynamicMapping bool `yaml:"dynamic_mapping"`
	CustomMapping  []struct {
		From string `yaml:"from"`
		To   string `yaml:"to"`
	} `yaml:"custom_mapping"`
}

type CSVAggregationRule struct {
	Operation string `yaml:"operation"`
	Column    string `yaml:"column"`
	As        string `yaml:"as"`
}

// ApplyCSVTransformations applies all transformations to CSV data
func ApplyCSVTransformations(filePath string, rules CSVTransformationRules) ([]map[string]interface{}, error) {
	// Read CSV data
	data, err := readCSV(filePath)
	if err != nil {
		return nil, err
	}

	// Apply Filters
	data = applyCSVFilters(data, rules.Filter)

	// Apply Dynamic Mapping
	if rules.Mapping.DynamicMapping {
		data = applyCSVDynamicMapping(data)
	}

	// Apply Aggregations
	data = applyCSVAggregations(data, rules.Aggregation)

	return data, nil
}

// Helper function to read CSV data
func readCSV(filePath string) ([]map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	headers := records[0]
	var data []map[string]interface{}

	for _, record := range records[1:] {
		row := make(map[string]interface{})
		for i, value := range record {
			row[headers[i]] = value
		}
		data = append(data, row)
	}

	return data, nil
}

// Helper function to apply filters to CSV data
func applyCSVFilters(data []map[string]interface{}, filters []CSVFilterRule) []map[string]interface{} {
	var filteredData []map[string]interface{}

	for _, row := range data {
		includeRow := true
		for _, filter := range filters {
			column := filter.Column
			condition := filter.Condition

			// Check if column exists
			if value, exists := row[column]; exists || column == "*" {
				// Apply numeric conditions if the value is a number
				if isCSVNumeric(value) {
					numValue, _ := strconv.ParseFloat(value.(string), 64)
					includeRow = includeRow && checkCSVCondition(numValue, condition)
				}
			}
		}
		if includeRow {
			filteredData = append(filteredData, row)
		}
	}

	return filteredData
}

// Helper function to apply dynamic mapping to CSV data
func applyCSVDynamicMapping(data []map[string]interface{}) []map[string]interface{} {
	var mappedData []map[string]interface{}

	for _, row := range data {
		mappedRow := make(map[string]interface{})
		for key, value := range row {
			mappedRow[strings.ToLower(key)] = value
		}
		mappedData = append(mappedData, mappedRow)
	}

	return mappedData
}

// Helper function to apply aggregations to CSV data
func applyCSVAggregations(data []map[string]interface{}, aggregations []CSVAggregationRule) []map[string]interface{} {
	if len(data) == 0 {
		return data
	}

	// Calculate aggregations
	for _, aggregation := range aggregations {
		operation := aggregation.Operation
		column := aggregation.Column
		newKey := aggregation.As

		if column == "*" { // Apply to all numeric columns
			for k := range data[0] {
				if isCSVNumeric(data[0][k]) {
					data = applySingleCSVAggregation(data, operation, k, newKey)
				}
			}
		} else { // Apply to specific column
			data = applySingleCSVAggregation(data, operation, column, newKey)
		}
	}

	return data
}

// Helper function to apply a single aggregation operation to CSV data
func applySingleCSVAggregation(data []map[string]interface{}, operation, column, newKeyPattern string) []map[string]interface{} {
	aggregateValue := 0.0
	count := 0

	// Compute aggregation
	for _, row := range data {
		if value, exists := row[column]; exists && isCSVNumeric(value) {
			numValue, _ := strconv.ParseFloat(value.(string), 64)
			switch operation {
			case "sum":
				aggregateValue += numValue
			case "count":
				count++
			}
		}
	}

	// Add the aggregation result to each row
	for i := range data {
		if operation == "sum" {
			data[i][strings.Replace(newKeyPattern, "<column>", column, 1)] = aggregateValue
		} else if operation == "count" {
			data[i][strings.Replace(newKeyPattern, "<column>", column, 1)] = count
		}
	}

	return data
}

// Helper function to check numeric condition for CSV data
func checkCSVCondition(value float64, condition string) bool {
	condition = strings.TrimSpace(condition)
	if strings.HasPrefix(condition, ">") {
		threshold, _ := strconv.ParseFloat(strings.TrimSpace(condition[1:]), 64)
		return value > threshold
	} else if strings.HasPrefix(condition, "<") {
		threshold, _ := strconv.ParseFloat(strings.TrimSpace(condition[1:]), 64)
		return value < threshold
	} else if strings.HasPrefix(condition, "=") {
		threshold, _ := strconv.ParseFloat(strings.TrimSpace(condition[1:]), 64)
		return value == threshold
	}
	return true
}

// Helper function to check if a value is numeric in CSV data
func isCSVNumeric(value interface{}) bool {
	_, ok := value.(string)
	return ok
}

// CSVToJSON converts a slice of map[string]interface{} (representing CSV data) into JSON format
func CSVToJSON(data []map[string]interface{}) []byte {
	// Marshal the CSV data into JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("error converting CSV to JSON: %v", err)
	}

	return jsonData
}
