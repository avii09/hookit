package config

import (
	"os"

	"github.com/avii09/hookit/pkg/transform"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Pipeline struct {
		Input struct {
			Type   string `yaml:"type"`
			Config struct {
				Collection string `yaml:"collection"`
				FilePath   string `yaml:"filePath"`
			} `yaml:"config"`
		} `yaml:"input"`
		Transformations transform.TransformationRules `yaml:"transformations"`
		Output struct {
			Type   string `yaml:"type"`
			Config struct {
				Collection string `yaml:"collection"`
				FilePath   string `yaml:"filePath"`
			} `yaml:"config"`
		} `yaml:"output"`
	} `yaml:"pipeline"`
}

// LoadConfig loads the configuration from a YAML file.
func LoadConfig(filePath string) (Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	var config Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, err
	}

	return config, nil
}
