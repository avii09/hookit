package main

import (
	"fmt"
	"log"
	"os"

	"github.com/avii09/hookit/pkg/input"
	"github.com/avii09/hookit/pkg/output"
	"github.com/avii09/hookit/pkg/transform"
	"gopkg.in/yaml.v2"
)

// Config structure for the pipeline
type Config struct {
	Pipeline struct {
		Input           map[string]interface{} `yaml:"input"`
		Transformations interface{}            `yaml:"transformations"`
		Output          map[string]interface{} `yaml:"output"`
	} `yaml:"pipeline"`
}

func main() {
	// Step 1: Load the YAML configuration file
	configFile, err := os.ReadFile("config/pipeline.yaml")
	if err != nil {
		log.Fatalf("Failed to read YAML config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("Failed to parse YAML config: %v", err)
	}

	// Step 2: Determine input type (CSV or JSON)
	inputType, ok := config.Pipeline.Input["type"].(string)
	if !ok {
		log.Fatalf("Input type is missing or invalid in configuration")
	}

	var data interface{}
	switch inputType {
	case "csv":
		inputConfig, ok := config.Pipeline.Input["config"].(map[interface{}]interface{})
		if !ok {
			log.Fatalf("Invalid input configuration format for CSV")
		}

		inputFilePath, ok := inputConfig["filePath"].(string)
		if !ok {
			log.Fatalf("Input file path is missing or invalid in configuration")
		}

		fmt.Printf("Reading input from CSV: %s\n", inputFilePath)
		data, err = input.ReadCSV(inputFilePath)
		if err != nil {
			log.Fatalf("Failed to read CSV input: %v", err)
		}

	case "json":
		inputConfig, ok := config.Pipeline.Input["config"].(map[interface{}]interface{})
		if !ok {
			log.Fatalf("Invalid input configuration format for JSON")
		}

		inputFilePath, ok := inputConfig["filePath"].(string)
		if !ok {
			log.Fatalf("Input file path is missing or invalid in configuration")
		}

		fmt.Printf("Reading input from JSON: %s\n", inputFilePath)
		data, err = input.ReadJSON(inputFilePath)
		if err != nil {
			log.Fatalf("Failed to read JSON input: %v", err)
		}

	default:
		log.Fatalf("Unsupported input type: %s", inputType)
	}

	// Step 3: Apply transformations based on the input type
	fmt.Println("Applying transformations...")
	var transformedData interface{}
	switch inputType {
	case "csv":
		transformedData = transform.ApplyTransformations(data.([]map[string]string), config.Pipeline.Transformations)
	case "json":
		transformedData = transform.ApplyJSONTransformations(data.([]map[string]interface{}), config.Pipeline.Transformations)
	}

	fmt.Println("Transformations applied successfully!")

	// Step 4: Extract output configuration and write output data
	outputConfig, ok := config.Pipeline.Output["config"].(map[interface{}]interface{})
	if !ok {
		log.Fatalf("Invalid output configuration format")
	}

	outputFilePath, ok := outputConfig["filePath"].(string)
	if !ok {
		log.Fatalf("Output file path is missing or invalid in configuration")
	}

	// Step 5: Write output based on the output type (CSV or JSON)
	outputType, ok := config.Pipeline.Output["type"].(string)
	if !ok {
		log.Fatalf("Output type is missing or invalid in configuration")
	}

	switch outputType {
	case "csv":
		fmt.Printf("Writing transformed data to CSV: %s\n", outputFilePath)
		err = output.WriteCSV(outputFilePath, transformedData)
		if err != nil {
			log.Fatalf("Failed to write output CSV: %v", err)
		}

	case "json":
		fmt.Printf("Writing transformed data to JSON: %s\n", outputFilePath)
		err = output.WriteJSON(outputFilePath, transformedData)
		if err != nil {
			log.Fatalf("Failed to write output JSON: %v", err)
		}

	default:
		log.Fatalf("Unsupported output type: %s", outputType)
	}

	fmt.Println("Data pipeline completed successfully!")
}
