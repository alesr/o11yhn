package kafkaconfig

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type Consumer struct {
	KafkaBrokers       []string `env:"KAFKA_BROKERS,required" envSeparator:","`
	KafkaTopic         string   `env:"KAFKA_TOPIC,required"`
	KafkaConsumerGroup string   `env:"KAFKA_CONSUMER_GROUP,required"`
	KafkaResetOffset   string   `env:"KAFKA_RESET_OFFSET" envDefault:"latest"`

	ClickHouseAddr     string `env:"CLICKHOUSE_ADDR,required"`
	ClickHouseUser     string `env:"CLICKHOUSE_USER,required"`
	ClickHousePassword string `env:"CLICKHOUSE_PASSWORD,required"`
	ClickHouseDatabase string `env:"CLICKHOUSE_DATABASE" envDefault:"default"`

	Dash0OTLPEndpoint  string `env:"DASH0_OTLP_ENDPOINT"`
	Dash0AuthToken     string `env:"DASH0_AUTH_TOKEN"`
	Dash0Metrics       string `env:"DASH0_METRICS_ENDPOINT"`
	Dash0EnableMetrics bool   `env:"DASH0_ENABLE_METRICS" envDefault:"false"`
}

func LoadConsumer() (Consumer, error) {
	if err := loadDotEnv(); err != nil {
		return Consumer{}, err
	}

	var cfg Consumer
	if err := env.Parse(&cfg); err != nil {
		return Consumer{}, fmt.Errorf("could not parse consumer config: %w", err)
	}

	normalizeCSV(&cfg.KafkaBrokers)
	if len(cfg.KafkaBrokers) == 0 {
		return Consumer{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	return cfg, nil
}
