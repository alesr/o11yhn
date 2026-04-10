package kafkaproducer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("github.com/alesr/o11yhn/internal/kafkaproducer")

type Producer struct {
	client *kgo.Client
	topic  string
}

func New(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka client: %w", err)
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("could not ping kafka brokers: %w", err)
	}
	return &Producer{
		client: client,
		topic:  topic,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, payload any) error {
	ctx, span := tracer.Start(ctx, "kafka.produce")
	defer span.End()

	span.SetAttributes(attribute.String("kafka.topic", p.topic))

	data, err := json.Marshal(payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal payload failed")
		return fmt.Errorf("could not marshal payload: %w", err)
	}

	record := kgo.Record{Topic: p.topic, Value: data}

	// waits for ack for to help with debugging during dev
	if err := p.client.ProduceSync(ctx, &record).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce sync failed")
		return fmt.Errorf("could not produce record: %w", err)
	}

	span.SetAttributes(attribute.Int("kafka.payload_bytes", len(data)))
	return nil
}

func (p *Producer) Close() { p.client.Close() }
