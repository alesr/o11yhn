package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/alesr/o11yhn/internal/engine"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("github.com/alesr/o11yhn/internal/kafkaconsumer")

type (
	PolledRecord struct {
		Record  *kgo.Record
		Mention *engine.HNMention
	}

	PollResult struct {
		Records []PolledRecord
	}
)

type Consumer struct{ client *kgo.Client }

func New(brokers []string, topic string, group string, resetOffset string) (*Consumer, error) {
	offset := kgo.NewOffset().AtEnd()

	if strings.EqualFold(resetOffset, "earliest") || strings.EqualFold(resetOffset, "start") {
		offset = kgo.NewOffset().AtStart()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(offset),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka consumer client: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) PollMentions(ctx context.Context) (PollResult, error) {
	ctx, span := tracer.Start(ctx, "kafka.poll")
	defer span.End()

	fetches := c.client.PollFetches(ctx)

	records := make([]PolledRecord, 0)

	var (
		validCount   int
		skippedCount int
	)

	errs := make([]error, 0)

	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}

		errs = append(errs, fmt.Errorf(
			"fetch error topic=%s partition=%d: %w",
			fetchErr.Topic, fetchErr.Partition, fetchErr.Err,
		))
	}

	fetches.EachRecord(
		func(record *kgo.Record) {
			var mention engine.HNMention

			if err := json.Unmarshal(record.Value, &mention); err != nil {
				errs = append(errs, fmt.Errorf(
					"skipping malformed record topic=%s partition=%d offset=%d: %w",
					record.Topic, record.Partition, record.Offset, err,
				))

				records = append(records, PolledRecord{
					Record:  record,
					Mention: nil,
				})

				skippedCount++
				return
			}

			mentionCopy := mention

			records = append(records, PolledRecord{
				Record:  record,
				Mention: &mentionCopy,
			})
			validCount++
		},
	)

	consumeErr := errors.Join(errs...)
	if consumeErr != nil {
		span.RecordError(consumeErr)
		span.SetStatus(codes.Error, "consume completed with errors")
	}

	span.SetAttributes(
		attribute.Int("kafka.records", validCount),
		attribute.Int("kafka.skipped_records", skippedCount),
		attribute.Int("kafka.error_count", len(errs)),
	)

	return PollResult{
		Records: records,
	}, consumeErr
}

func (c *Consumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("could not commit records: %w", err)
	}
	return nil
}

func (c *Consumer) Close() { c.client.Close() }
