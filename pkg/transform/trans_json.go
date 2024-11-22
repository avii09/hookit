package transform

import (
	"strconv"
	"strings"
)

// Define transformation rules structures for JSON
type JSONTransformationRules struct {
	Filter      []JSONFilterRule      `yaml:"filter"`
	Mapping     JSONMappingRules      `yaml:"mapping"`
	Aggregation []JSONAggregationRule `yaml:"aggregation"`
}

type JSONFilterRule struct {
	Key       string `yaml:"key"`
	Condition string `yaml:"condition"`
}

type JSONMappingRules struct {
	DynamicMapping bool `yaml:"dynamic_mapping"`
	CustomMapping  []struct {
		From string `yaml:"from"`
		To   string `yaml:"to"`
	} `yaml:"custom_mapping"`
}

type JSONAggregationRule struct {
	Operation string `yaml:"operation"`
	Key       string `yaml:"key"`
	As        string `yaml:"as"`
}

// ApplyJSONTransformations applies all transformations to JSON data
func ApplyJSONTransformations(data []map[string]interface{}, rules JSONTransformationRules) []map[string]interface{} {
	// Apply Filters
	data = applyJSONFilters(data, rules.Filter)

	// Apply Dynamic Mapping
	if rules.Mapping.DynamicMapping {
		data = applyJSONDynamicMapping(data)
	}

	// Apply Aggregations
	data = applyJSONAggregations(data, rules.Aggregation)

	return data
}

// Helper function to apply filters to JSON data
func applyJSONFilters(data []map[string]interface{}, filters []JSONFilterRule) []map[string]interface{} {
	var filteredData []map[string]interface{}

	for _, row := range data {
		includeRow := true
		for _, filter := range filters {
			key := filter.Key
			condition := filter.Condition

			// Check if key exists
			if value, exists := row[key]; exists || key == "*" {
				// Apply numeric conditions if the value is a number
				if isJSONNumeric(value) {
					numValue, _ := value.(float64)
					includeRow = includeRow && checkJSONCondition(numValue, condition)
				}
			}
		}
		if includeRow {
			filteredData = append(filteredData, row)
		}
	}

	return filteredData
}

// Helper function to apply dynamic mapping to JSON data
func applyJSONDynamicMapping(data []map[string]interface{}) []map[string]interface{} {
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

// Helper function to apply aggregations to JSON data
func applyJSONAggregations(data []map[string]interface{}, aggregations []JSONAggregationRule) []map[string]interface{} {
	if len(data) == 0 {
		return data
	}

	// Calculate aggregations
	for _, aggregation := range aggregations {
		operation := aggregation.Operation
		key := aggregation.Key
		newKey := aggregation.As

		if key == "*" { // Apply to all numeric keys
			for k := range data[0] {
				if isJSONNumeric(data[0][k]) {
					data = applySingleJSONAggregation(data, operation, k, newKey)
				}
			}
		} else { // Apply to specific key
			data = applySingleJSONAggregation(data, operation, key, newKey)
		}
	}

	return data
}

// Helper function to apply a single aggregation operation to JSON data
func applySingleJSONAggregation(data []map[string]interface{}, operation, key, newKeyPattern string) []map[string]interface{} {
	aggregateValue := 0.0
	count := 0

	// Compute aggregation
	for _, row := range data {
		if value, exists := row[key]; exists && isJSONNumeric(value) {
			numValue := value.(float64)
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
			data[i][strings.Replace(newKeyPattern, "<key>", key, 1)] = aggregateValue
		} else if operation == "count" {
			data[i][strings.Replace(newKeyPattern, "<key>", key, 1)] = count
		}
	}

	return data
}

// Helper function to check numeric condition for JSON data
func checkJSONCondition(value float64, condition string) bool {
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

// Helper function to check if a value is numeric in JSON data
func isJSONNumeric(value interface{}) bool {
	_, ok := value.(float64)
	return ok
}
