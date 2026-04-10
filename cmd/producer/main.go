package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alesr/o11yhn/internal/engine"
	"github.com/alesr/o11yhn/internal/hnclient"
	"github.com/alesr/o11yhn/internal/kafkaproducer"
	"github.com/alesr/o11yhn/internal/watermark"
	"github.com/alesr/o11yhn/pkg/cmdutil"
	"github.com/alesr/o11yhn/pkg/kafkaconfig"
	"github.com/alesr/o11yhn/pkg/telemetry"
	"github.com/alesr/workerpool"
	"github.com/hashicorp/go-retryablehttp"
	"go.opentelemetry.io/otel"
)

const (
	defaultHTTPClientTimeout = time.Second * 10
	workerCount              = 10
	maxIdleConns             = 20
	maxIdleConnsPerHost      = 20
	serviceName              = "o11yhn-producer"
)

func main() {
	logger := cmdutil.NewLogger(slog.LevelInfo).WithGroup(serviceName)
	cfg := cmdutil.Must(kafkaconfig.LoadProducer())

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

	_, startupSpan := otel.Tracer("github.com/alesr/o11yhn/cmd/producer").Start(ctx, "service.startup")
	startupSpan.End()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(shutdownCtx); err != nil {
			logger.Error("Telemetry shutdown failed", "error", err)
		}
	}()

	// setup HTTP client

	transport := http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.RetryWaitMin = 1 * time.Second
	retryClient.RetryWaitMax = 5 * time.Second
	retryClient.Logger = logger.WithGroup("hnclient")
	retryClient.HTTPClient.Transport = &transport
	retryClient.HTTPClient.Timeout = defaultHTTPClientTimeout

	// setup hacker news client

	hnClient := cmdutil.Must(hnclient.New(retryClient.StandardClient()))

	// setup Kafka producer

	producer := cmdutil.Must(
		kafkaproducer.New(cfg.KafkaBrokers, cfg.KafkaTopic),
	)

	defer producer.Close()

	logger.Info("Connected to Redpanda", "brokers", strings.Join(cfg.KafkaBrokers, ","), "topic", cfg.KafkaTopic)

	// setup worker pool and engine for fetching hn items and processing them

	pool := workerpool.New[engine.FetchTaskInput](workerCount)
	defer pool.Shutdown()

	wmStore := watermark.NewFileStore(cfg.ProducerWatermarkPath)

	eng := cmdutil.Must(
		engine.NewEngine(
			logger,
			hnClient,
			producer,
			wmStore,
			pool,
			engine.Config{
				Keywords:        cfg.EngineKeywords,
				HistoryLookback: cfg.EngineHistoryLookback,
			},
		),
	)

	logger.Info("Starting engine...")

	// block until ctx is done via cancellation signal
	eng.Start(ctx)

	logger.Info("Engine shutdown complete")
}
