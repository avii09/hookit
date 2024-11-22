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
        Input           map[string]interface{}        `yaml:"input"`
        Transformations transform.TransformationRules `yaml:"transformations"`
        Output          map[string]interface{}        `yaml:"output"`
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

    // Step 2: Extract input configuration and read input data
    inputConfig, ok := config.Pipeline.Input["config"].(map[interface{}]interface{})
    if !ok {
        log.Fatalf("Invalid input configuration format")
    }

    inputFilePath, ok := inputConfig["filePath"].(string)
    if !ok {
        log.Fatalf("Input file path is missing or invalid in configuration")
    }

    fmt.Printf("Reading input from: %s\n", inputFilePath)
    data, err := input.ReadCSV(inputFilePath)
    if err != nil {
        log.Fatalf("Failed to read input CSV: %v", err)
    }
    fmt.Println("Input data successfully read!")

    // Step 3: Apply transformations
    fmt.Println("Applying transformations...")
    transformedData := transform.ApplyTransformations(data, config.Pipeline.Transformations)
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

    fmt.Printf("Writing transformed data to: %s\n", outputFilePath)
    err = output.WriteCSV(outputFilePath, transformedData)
    if err != nil {
        log.Fatalf("Failed to write output CSV: %v", err)
    }
    fmt.Println("Data pipeline completed successfully!")
}
