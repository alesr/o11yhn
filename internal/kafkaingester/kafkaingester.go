package kafkaingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/alesr/o11yhn/internal/engine"
	"github.com/alesr/o11yhn/internal/kafkaconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBatchSize                  = 100
	defaultFlushInterval              = 5 * time.Second
	defaultPollTimeout                = 1 * time.Second
	defaultShutdownFlushTimeout       = 15 * time.Second
	defaultShutdownFlushRetryInterval = 500 * time.Millisecond
)

type (
	mentionConsumer interface {
		PollMentions(ctx context.Context) (kafkaconsumer.PollResult, error)
		CommitRecords(ctx context.Context, records ...*kgo.Record) error
	}

	mentionStorage interface {
		BatchInsert(ctx context.Context, mentions []engine.HNMention) error
	}

	bufferedRecord struct {
		record  *kgo.Record
		mention *engine.HNMention
	}
)

type Ingester struct {
	logger   *slog.Logger
	consumer mentionConsumer
	storage  mentionStorage

	batchSize                  int
	flushInterval              time.Duration
	shutdownFlushTimeout       time.Duration
	shutdownFlushRetryInterval time.Duration
}

func New(logger *slog.Logger, consumer mentionConsumer, storage mentionStorage) *Ingester {
	return &Ingester{
		logger:                     logger.WithGroup("kafka_ingester"),
		consumer:                   consumer,
		storage:                    storage,
		batchSize:                  defaultBatchSize,
		flushInterval:              defaultFlushInterval,
		shutdownFlushTimeout:       defaultShutdownFlushTimeout,
		shutdownFlushRetryInterval: defaultShutdownFlushRetryInterval,
	}
}

func (ing *Ingester) Run(ctx context.Context) {
	ing.logger.Info("Consumer started. Waiting for messages...")

	var buffer []bufferedRecord

	flushTicker := time.NewTicker(ing.flushInterval)
	defer flushTicker.Stop()

	pollTicker := time.NewTicker(defaultPollTimeout)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			ing.flushOnShutdown(&buffer)
			return
		case <-flushTicker.C:
			if len(buffer) > 0 {
				if err := ing.flush(ctx, &buffer); err != nil {
					// avoid tight-loop retries when downstream is unavailable
					time.Sleep(ing.shutdownFlushRetryInterval)
				}
			}
		case <-pollTicker.C:
			pollCtx, cancel := context.WithTimeout(ctx, defaultPollTimeout)
			result, err := ing.consumer.PollMentions(pollCtx)
			cancel()

			if err != nil {
				ing.logger.Error("Kafka poll completed with errors", "error", err)
			}

			var flushFailed bool

			for _, record := range result.Records {
				buffer = append(buffer, bufferedRecord{
					record:  record.Record,
					mention: record.Mention,
				})

				if flushFailed {
					continue
				}

				if len(buffer) >= ing.batchSize {
					if err := ing.flush(ctx, &buffer); err != nil {
						flushFailed = true
					}
				}
			}

			if flushFailed {
				// backoff before the next poll cycle when downstream fails
				time.Sleep(ing.shutdownFlushRetryInterval)
			}
		}
	}
}

func (ing *Ingester) flushOnShutdown(buffer *[]bufferedRecord) {
	if len(*buffer) == 0 {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), ing.shutdownFlushTimeout)
	defer cancel()

	for len(*buffer) > 0 {
		if err := ing.flush(shutdownCtx, buffer); err == nil {
			return
		}

		select {
		case <-shutdownCtx.Done():
			ing.logger.Error("Failed to flush buffered mentions before shutdown", "error", shutdownCtx.Err(), "remaining", len(*buffer))
			return
		case <-time.After(ing.shutdownFlushRetryInterval):
		}
	}
}

func (ing *Ingester) flush(ctx context.Context, buffer *[]bufferedRecord) error {
	if len(*buffer) == 0 {
		return nil
	}

	mentions := make([]engine.HNMention, 0, len(*buffer))
	records := make([]*kgo.Record, 0, len(*buffer))

	var skippedCount int

	for _, item := range *buffer {
		records = append(records, item.record)

		if item.mention == nil {
			skippedCount++
			continue
		}
		mentions = append(mentions, *item.mention)
	}

	if len(mentions) > 0 {
		if err := ing.storage.BatchInsert(ctx, mentions); err != nil {
			ing.logger.Error("Batch insert failed", "error", err)
			return err
		}
	}

	if err := ing.consumer.CommitRecords(ctx, records...); err != nil {
		ing.logger.Error("Kafka commit failed after successful insert", "error", err)
		return err
	}

	if skippedCount > 0 {
		ing.logger.Warn("Committed malformed records after flush", "skipped", skippedCount)
	}

	ing.logger.Info("Successfully flushed and committed batch", "count", len(*buffer), "inserted", len(mentions))

	*buffer = (*buffer)[:0]
	return nil
}
