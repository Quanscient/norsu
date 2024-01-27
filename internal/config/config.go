package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Version    int         `yaml:"version"`
	Package    Package     `yaml:"package"`
	Queries    []Query     `yaml:"queries"`
	Migrations []Migration `yaml:"migrations"`
	Models     []Model     `yaml:"models"`
}

type Package struct {
	Path string `yaml:"path"`
}

type Query struct {
	Path string `yaml:"path"`
}

type Migration struct {
	Path string `yaml:"path"`
}

type Model struct {
	OpenApi OpenApi `yaml:"openApi"`
	Package Package `yaml:"package"`
}

type OpenApi struct {
	Path string `yaml:"path"`
}

func Read(configPath string) (*Config, error) {
	fileData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf(`failed to read config file "%s": %w`, configPath, err)
	}

	var config Config
	if err := yaml.Unmarshal(fileData, &config); err != nil {
		return nil, fmt.Errorf(`failed to unmarshal config file "%s": %w`, configPath, err)
	}

	return &config, nil
}
