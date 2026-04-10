package telemetry

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Config struct {
	OTLPEndpoint        string
	AuthToken           string
	ServiceName         string
	MetricsEndpoint     string
	EnableMetricsExport bool
}

type Telemetry struct{ shutdown func(context.Context) error }

func New(ctx context.Context, cfg Config) (*Telemetry, error) {
	if cfg.OTLPEndpoint == "" || cfg.AuthToken == "" || cfg.ServiceName == "" {
		return nil, fmt.Errorf("missing required telemetry config")
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create resource: %w", err)
	}

	traceExp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(cfg.OTLPEndpoint),
		otlptracehttp.WithHeaders(map[string]string{
			"Authorization": "Bearer " + cfg.AuthToken,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	var mp *metric.MeterProvider

	if cfg.EnableMetricsExport && cfg.MetricsEndpoint != "" {
		metricExp, metricErr := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpointURL(cfg.MetricsEndpoint),
			otlpmetricgrpc.WithHeaders(map[string]string{
				"Authorization": "Bearer " + cfg.AuthToken,
			}),
		)

		if metricErr != nil {
			if shutdownErr := tp.Shutdown(ctx); shutdownErr != nil {
				return nil, errors.Join(
					fmt.Errorf("could not create metric exporter: %w", metricErr),
					fmt.Errorf("could not shutdown trace provider after metric init failure: %w", shutdownErr),
				)
			}
			return nil, fmt.Errorf("could not create metric exporter: %w", metricErr)
		}

		mp = metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(metricExp)),
			metric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
	}

	return &Telemetry{
		shutdown: func(ctx context.Context) error {
			var shutdownErr error
			if mp != nil {
				shutdownErr = errors.Join(shutdownErr, mp.Shutdown(ctx))
			}

			shutdownErr = errors.Join(shutdownErr, tp.Shutdown(ctx))
			return shutdownErr
		},
	}, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.shutdown == nil {
		return nil
	}
	return t.shutdown(ctx)
}
