package kafkaconfig

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type Producer struct {
	KafkaBrokers          []string      `env:"KAFKA_BROKERS,required" envSeparator:","`
	KafkaTopic            string        `env:"KAFKA_TOPIC,required"`
	EngineKeywords        []string      `env:"ENGINE_KEYWORDS,required" envSeparator:","`
	EngineHistoryLookback time.Duration `env:"ENGINE_HISTORY_LOOKBACK,required"`
	ProducerWatermarkPath string        `env:"PRODUCER_WATERMARK_PATH"`

	Dash0OTLPEndpoint  string `env:"DASH0_OTLP_ENDPOINT"`
	Dash0AuthToken     string `env:"DASH0_AUTH_TOKEN"`
	Dash0Metrics       string `env:"DASH0_METRICS_ENDPOINT"`
	Dash0EnableMetrics bool   `env:"DASH0_ENABLE_METRICS" envDefault:"false"`
}

func LoadProducer() (Producer, error) {
	if err := loadDotEnv(); err != nil {
		return Producer{}, err
	}

	var cfg Producer
	if err := env.Parse(&cfg); err != nil {
		return Producer{}, fmt.Errorf("could not parse producer config: %w", err)
	}

	normalizeCSV(&cfg.KafkaBrokers)
	normalizeCSV(&cfg.EngineKeywords)

	if len(cfg.KafkaBrokers) == 0 {
		return Producer{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	if len(cfg.EngineKeywords) == 0 {
		return Producer{}, fmt.Errorf("ENGINE_KEYWORDS must contain at least one keyword")
	}
	if cfg.EngineHistoryLookback <= 0 {
		return Producer{}, fmt.Errorf("ENGINE_HISTORY_LOOKBACK must be greater than zero")
	}

	return cfg, nil
}
