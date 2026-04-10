package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/alesr/o11yhn/internal/hnclient"
	"github.com/alesr/o11yhn/internal/watermark"
	"github.com/alesr/workerpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultPoolingInterval = 1 * time.Minute
	defaultOverlapWindow   = 2 * time.Minute
	defaultSaveInterval    = 5 * time.Second
)

var tracer = otel.Tracer("github.com/alesr/o11yhn/internal/engine")

type (
	kafkaProducer interface {
		Produce(ctx context.Context, payload any) error
	}

	hnClient interface {
		Search(ctx context.Context, query string, since int64) ([]hnclient.AlgoliaHit, error)
	}
)

type Engine struct {
	logger          *slog.Logger
	hnClient        hnClient
	kafkaProducer   kafkaProducer
	watermark       watermark.Store
	historyLookback time.Duration
	pool            *workerpool.Pool[FetchTaskInput]
	lastTime        int64
	lastSavedTime   int64
	rules           map[string]*regexp.Regexp
	keywordOrder    []string
	deduper         *idDeduper
	tracker         *watermarkTracker
}

type Config struct {
	Keywords        []string
	HistoryLookback time.Duration
}

func NewEngine(
	logger *slog.Logger,
	hnClient hnClient,
	kafkaProducer kafkaProducer,
	watermarkStore watermark.Store,
	pool *workerpool.Pool[FetchTaskInput],
	cfg Config,
) (*Engine, error) {
	if len(cfg.Keywords) == 0 {
		return nil, fmt.Errorf("engine keywords cannot be empty")
	}

	if cfg.HistoryLookback <= 0 {
		return nil, fmt.Errorf("engine history lookback must be greater than zero")
	}

	rules := make(map[string]*regexp.Regexp)
	for _, kw := range cfg.Keywords {
		pattern := fmt.Sprintf(`(?i)\b%s\b`, regexp.QuoteMeta(kw))
		rules[kw] = regexp.MustCompile(pattern)
	}

	return &Engine{
		logger:          logger.WithGroup("engine"),
		hnClient:        hnClient,
		kafkaProducer:   kafkaProducer,
		watermark:       watermarkStore,
		historyLookback: cfg.HistoryLookback,
		pool:            pool,
		rules:           rules,
		keywordOrder:    cfg.Keywords,
		deduper:         newIDDeduper(),
		tracker:         newWatermarkTracker(0),
	}, nil
}

func (e *Engine) Start(ctx context.Context) {
	e.lastTime = e.loadLastTime(ctx)
	e.tracker.MarkProcessed(e.lastTime)
	e.lastSavedTime = e.lastTime
	e.logger.Info("Starting ingestion", "since", time.Unix(e.lastTime, 0))

	e.poll(ctx)

	pollTicker := time.NewTicker(defaultPoolingInterval)
	defer pollTicker.Stop()

	saveTicker := time.NewTicker(defaultSaveInterval)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// drain in-flight tasks before persisting the final watermark
			e.pool.Shutdown()
			e.persistWatermark(context.Background())
			return
		case <-pollTicker.C:
			e.poll(ctx)
		case <-saveTicker.C:
			e.persistWatermark(ctx)
		}
	}
}

func (e *Engine) loadLastTime(ctx context.Context) int64 {
	if e.watermark == nil {
		fallback := time.Now().Add(-e.historyLookback).Unix()
		e.logger.Info("Watermark store not configured, using historical lookback", "since", time.Unix(fallback, 0))
		return fallback
	}

	lastTime, err := e.watermark.Load(ctx)
	if err == nil {
		e.logger.Info("Loaded persisted watermark", "last_time", time.Unix(lastTime, 0))
		return lastTime
	}

	if !errors.Is(err, os.ErrNotExist) {
		e.logger.Error("Could not load persisted watermark, using historical lookback", "error", err)
	}

	fallback := time.Now().Add(-e.historyLookback).Unix()
	return fallback
}

func (e *Engine) persistWatermark(ctx context.Context) {
	if e.watermark == nil {
		return
	}

	current := e.tracker.LastTime()
	if current <= e.lastSavedTime {
		return
	}

	if err := e.watermark.Save(ctx, current); err != nil {
		e.logger.Error("Could not persist watermark", "error", err)
		return
	}
	e.lastSavedTime = current
}

func (e *Engine) poll(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "engine.poll")
	defer span.End()

	querySince := max(e.lastTime-int64(defaultOverlapWindow.Seconds()), 0)
	querySince = e.deduper.retrySince(querySince)

	span.SetAttributes(
		attribute.Int64("engine.last_time", e.lastTime),
		attribute.Int64("engine.query_since", querySince),
	)

	e.deduper.prune(querySince)
	pollSeen := make(map[int]struct{})

	var (
		totalHits      int
		totalSubmitted int
	)

	for _, kw := range e.keywordOrder {
		hits, err := e.hnClient.Search(ctx, kw, querySince)
		if err != nil {
			span.RecordError(err)
			e.logger.Error("Search error", "keyword", kw, "error", err)
			continue
		}
		totalHits += len(hits)

		for _, hit := range hits {
			if _, ok := pollSeen[hit.ID]; ok {
				continue
			}

			if e.deduper.seenInWindow(hit.ID, querySince) {
				continue
			}

			if !e.deduper.markPending(hit.ID, hit.CreatedAt) {
				continue
			}

			task := workerpool.Task[FetchTaskInput]{
				Fn:    func(input FetchTaskInput) { input.Do(ctx) },
				Input: newFetchTaskInput(e.logger, hit, e.rules, e.keywordOrder, e.kafkaProducer, e.deduper, e.tracker),
			}

			if !e.pool.Submit(task) {
				e.deduper.markFailed(hit.ID, hit.CreatedAt)
				e.logger.Warn("Worker pool rejected task submission", "id", hit.ID)
				continue
			}

			pollSeen[hit.ID] = struct{}{}
			totalSubmitted++
		}
	}

	processedLastTime := e.tracker.LastTime()

	if processedLastTime > e.lastTime {
		e.lastTime = processedLastTime
	}

	span.SetAttributes(
		attribute.Int("engine.total_hits", totalHits),
		attribute.Int("engine.total_submitted", totalSubmitted),
		attribute.Int64("engine.updated_last_time", e.lastTime),
	)
}
