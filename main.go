package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	firebase "firebase.google.com/go"
	"github.com/avii09/hookit/pkg/config"
	"github.com/avii09/hookit/pkg/input"
	"github.com/avii09/hookit/pkg/output"
	"github.com/avii09/hookit/pkg/transform"
	"google.golang.org/api/option"
)

func main() {
    // Define the pipeline type flag
    pipelineType := flag.String("pipeline", "csv", "Specify the pipeline type: csv or firebase")
    flag.Parse()

    var configFilePath string
    if *pipelineType == "firebase" {
        configFilePath = "config/firebase.yaml"
    } else {
        configFilePath = "config/csv.yaml"
    }

    // Load the configuration file
    cfg, err := config.LoadConfig(configFilePath)
    if err != nil {
        log.Fatalf("error loading config file: %v", err)
    }

    if *pipelineType == "firebase" {
        runFirebasePipeline(cfg)
    } else {
        runCSVPipeline(cfg)
    }
}

func runFirebasePipeline(cfg config.Config) {
    // Initialize Firebase app
    opt := option.WithCredentialsFile("firebase-adminsdk.json")
    app, err := firebase.NewApp(context.Background(), nil, opt)
    if err != nil {
        log.Fatalf("error initializing app: %v", err)
    }

    // Initialize Firestore client
    client, err := app.Firestore(context.Background())
    if err != nil {
        log.Fatalf("error initializing Firestore: %v", err)
    }
    defer client.Close()

    // Read data from Firebase
    data, err := input.ReadFirebase(client, cfg.Pipeline.Input.Config.Collection)
    if err != nil {
        log.Fatalf("error reading data from Firebase: %v", err)
    }

    // Apply transformations
    transformedData := transform.ApplyFirebaseTransformations(data, cfg.Pipeline.Transformations.Mapping)

    // Write data back to Firebase
    if err := output.WriteFirebase(client, cfg.Pipeline.Output.Config.Collection, transformedData); err != nil {
        log.Fatalf("error writing data to Firebase: %v", err)
    }

    fmt.Println("Data transformed and written to Firebase successfully!")
}

func runCSVPipeline(cfg config.Config) {
    // Read data from CSV
    data, err := input.ReadCSV(cfg.Pipeline.Input.Config.FilePath)
    if err != nil {
        log.Fatalf("error reading data from CSV: %v", err)
    }

    // Apply transformations
    transformedData := transform.ApplyTransformations(data, cfg.Pipeline.Transformations)

    // Write data to CSV
    if err := output.WriteCSV(cfg.Pipeline.Output.Config.FilePath, transformedData); err != nil {
        log.Fatalf("error writing data to CSV: %v", err)
    }

    fmt.Println("Data transformed and written to CSV successfully!")
}
