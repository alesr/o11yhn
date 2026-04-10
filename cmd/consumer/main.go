package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alesr/o11yhn/internal/kafkaconsumer"
	"github.com/alesr/o11yhn/internal/kafkaingester"
	"github.com/alesr/o11yhn/internal/storage"
	"github.com/alesr/o11yhn/pkg/cmdutil"
	"github.com/alesr/o11yhn/pkg/kafkaconfig"
	"github.com/alesr/o11yhn/pkg/telemetry"
	"go.opentelemetry.io/otel"
)

const serviceName = "o11yhn-consumer"

func main() {
	logger := cmdutil.NewLogger(slog.LevelInfo).WithGroup("cmd_consumer")

	cfg := cmdutil.Must(kafkaconfig.LoadConsumer())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// setup telemetry

	var tel *telemetry.Telemetry
	if cfg.Dash0OTLPEndpoint == "" || cfg.Dash0AuthToken == "" {
		logger.Warn("Telemetry disabled because endpoint or token is missing")
	} else {
		if cfg.Dash0EnableMetrics && cfg.Dash0Metrics == "" {
			logger.Warn("Metrics export requested but metrics endpoint is empty; metrics will be disabled")
		}

		tel = cmdutil.Must(
			telemetry.New(
				ctx,
				telemetry.Config{
					OTLPEndpoint:        cfg.Dash0OTLPEndpoint,
					AuthToken:           cfg.Dash0AuthToken,
					ServiceName:         serviceName,
					MetricsEndpoint:     cfg.Dash0Metrics,
					EnableMetricsExport: cfg.Dash0EnableMetrics,
				},
			),
		)
		logger.Info("Telemetry initialized", "service_name", serviceName)
	}

	_, startupSpan := otel.Tracer("github.com/alesr/o11yhn/cmd/consumer").Start(ctx, "service.startup")
	startupSpan.End()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(shutdownCtx); err != nil {
			logger.Error("Telemetry shutdown failed", "error", err)
		}
	}()

	// setup ClickHouse storage

	ch, err := storage.NewClickHouse(cfg.ClickHouseAddr, cfg.ClickHouseUser, cfg.ClickHousePassword, cfg.ClickHouseDatabase)
	if err != nil {
		logger.Error("ClickHouse connection failed", "error", err)
		os.Exit(1)
	}

	if err := ch.EnsureSchema(ctx); err != nil {
		logger.Error("ClickHouse schema setup failed", "error", err)
		os.Exit(1)
	}

	// setup Kafka consumer

	consumer, err := kafkaconsumer.New(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		cfg.KafkaResetOffset,
	)
	if err != nil {
		logger.Error("Kafka consumer failed", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// setup ingester service for consuming Kafka messages and storing them in ClickHouse

	runner := kafkaingester.New(logger, consumer, ch)
	runner.Run(ctx)
}
