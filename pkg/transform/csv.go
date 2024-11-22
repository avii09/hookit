package transform

import (
	"strconv"
	"strings"
)

// Define transformation rules structures
type TransformationRules struct {
    Filter      []FilterRule      `yaml:"filter"`
    Mapping     MappingRules      `yaml:"mapping"`
    Aggregation []AggregationRule `yaml:"aggregation"`
}

type FilterRule struct {
    Column    string `yaml:"column"`
    Condition string `yaml:"condition"`
}

type MappingRules struct {
    NameChange     string            `yaml:"name_change"`
    AddField       map[string]string `yaml:"add_field"`
    DynamicMapping bool              `yaml:"dynamic_mapping"`
}

type AggregationRule struct {
    Operation string `yaml:"operation"`
    Column    string `yaml:"column"`
    As        string `yaml:"as"`
}

// ApplyTransformations applies all transformations to the data
func ApplyTransformations(data []map[string]string, rules TransformationRules) []map[string]string {
    // Apply Filters
    data = applyFilters(data, rules.Filter)

    // Apply Dynamic Mapping
    if rules.Mapping.DynamicMapping {
        data = applyDynamicMapping(data)
    }

    // Apply Aggregations
    data = applyAggregations(data, rules.Aggregation)

    return data
}

// Helper function to apply filters
func applyFilters(data []map[string]string, filters []FilterRule) []map[string]string {
    var filteredData []map[string]string

    for _, row := range data {
        includeRow := true
        for _, filter := range filters {
            column := filter.Column
            condition := filter.Condition

            // Check if column exists
            if value, exists := row[column]; exists || column == "*" {
                // Apply numeric conditions if the value is a number
                if isNumeric(value) {
                    numValue, _ := strconv.ParseFloat(value, 64)
                    includeRow = includeRow && checkCondition(numValue, condition)
                }
            }
        }
        if includeRow {
            filteredData = append(filteredData, row)
        }
    }

    return filteredData
}

// Helper function to apply dynamic mapping (e.g., lowercase column names)
func applyDynamicMapping(data []map[string]string) []map[string]string {
    var mappedData []map[string]string

    for _, row := range data {
        mappedRow := make(map[string]string)
        for column, value := range row {
            mappedRow[strings.ToLower(column)] = value
        }
        mappedData = append(mappedData, mappedRow)
    }

    return mappedData
}

// Helper function to apply aggregations
func applyAggregations(data []map[string]string, aggregations []AggregationRule) []map[string]string {
    if len(data) == 0 {
        return data
    }

    // Calculate aggregations
    for _, aggregation := range aggregations {
        operation := aggregation.Operation
        column := aggregation.Column
        newColumn := aggregation.As

        if column == "*" { // Apply to all numeric columns
            for key := range data[0] {
                if isNumeric(data[0][key]) {
                    data = applySingleAggregation(data, operation, key, newColumn)
                }
            }
        } else { // Apply to specific column
            data = applySingleAggregation(data, operation, column, newColumn)
        }
    }

    return data
}

// Helper function to apply a single aggregation operation
func applySingleAggregation(data []map[string]string, operation, column, newColumnPattern string) []map[string]string {
    aggregateValue := 0.0
    count := 0

    // Compute aggregation
    for _, row := range data {
        if value, exists := row[column]; exists && isNumeric(value) {
            numValue, _ := strconv.ParseFloat(value, 64)
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
            data[i][strings.Replace(newColumnPattern, "<column>", column, 1)] = strconv.FormatFloat(aggregateValue, 'f', 2, 64)
        } else if operation == "count" {
            data[i][strings.Replace(newColumnPattern, "<column>", column, 1)] = strconv.Itoa(count)
        }
    }

    return data
}

// Helper function to check numeric condition
func checkCondition(value float64, condition string) bool {
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

// Helper function to check if a string is numeric
func isNumeric(value string) bool {
    _, err := strconv.ParseFloat(value, 64)
    return err == nil
}
