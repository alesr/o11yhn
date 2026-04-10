package engine

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/alesr/o11yhn/internal/hnclient"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var htmlRegex = regexp.MustCompile(`<[^>]*>`)

const defaultProduceTimeout = 10 * time.Second

type FetchTaskInput struct {
	logger       *slog.Logger
	Hit          hnclient.AlgoliaHit
	Rules        map[string]*regexp.Regexp
	KeywordOrder []string
	Producer     kafkaProducer
	Deduper      *idDeduper
	Watermark    *watermarkTracker
}

func newFetchTaskInput(logger *slog.Logger,
	hit hnclient.AlgoliaHit,
	rules map[string]*regexp.Regexp,
	keywordOrder []string,
	producer kafkaProducer,
	deduper *idDeduper,
	wm *watermarkTracker,
) FetchTaskInput {
	return FetchTaskInput{
		logger:       logger,
		Hit:          hit,
		Rules:        rules,
		KeywordOrder: keywordOrder,
		Producer:     producer,
		Deduper:      deduper,
		Watermark:    wm,
	}
}

func (f FetchTaskInput) Do(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "engine.process_hit")
	defer span.End()
	span.SetAttributes(attribute.Int("hn.item.id", f.Hit.ID))

	title := f.Hit.Title
	if title == "" {
		title = f.Hit.StoryTitle
	}

	cleanText := cleanHTML(f.Hit.Text)
	fullContent := title + " " + cleanText

	matchedKeywords := make([]string, 0, len(f.KeywordOrder))

	for _, kw := range f.KeywordOrder {
		re, ok := f.Rules[kw]
		if !ok {
			continue
		}
		if re.MatchString(fullContent) {
			matchedKeywords = append(matchedKeywords, kw)
		}
	}

	if len(matchedKeywords) == 0 {
		if f.Deduper != nil {
			// mark irrelevant hits as processed in dedupe without advancing watermark
			f.Deduper.markProcessed(f.Hit.ID, f.Hit.CreatedAt)
		}
		span.SetAttributes(attribute.Bool("mention.relevant", false))
		return
	}

	matchedKeyword := matchedKeywords[0]

	// determine type
	itemType := "comment"
	if slices.Contains(f.Hit.Tags, "story") {
		itemType = "story"
	}

	// build HN standard format url
	url := fmt.Sprintf("https://news.ycombinator.com/item?id=%d", f.Hit.ID)

	// event
	mention := HNMention{
		ID:              f.Hit.ID,
		Type:            itemType,
		MatchedKeyword:  matchedKeyword,
		MatchedKeywords: matchedKeywords,
		Title:           title,
		Text:            cleanText,
		Author:          f.Hit.Author,
		URL:             url,
		CreatedAt:       time.Unix(f.Hit.CreatedAt, 0),
	}

	produceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultProduceTimeout)
	defer cancel()

	if err := f.Producer.Produce(produceCtx, mention); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "kafka produce failed")
		if f.Deduper != nil {
			f.Deduper.markFailed(mention.ID, f.Hit.CreatedAt)
		}

		f.logger.Error("Failed to produce to Kafka", "id", mention.ID, "error", err)
		return
	}

	if f.Deduper != nil {
		f.Deduper.markProcessed(mention.ID, f.Hit.CreatedAt)
	}

	if f.Watermark != nil {
		f.Watermark.MarkProcessed(f.Hit.CreatedAt)
	}

	span.SetAttributes(
		attribute.Bool("mention.relevant", true),
		attribute.String("mention.type", mention.Type),
		attribute.String("mention.matched_keyword", mention.MatchedKeyword),
	)
	f.logger.Info("Published mention", "id", mention.ID, "author", mention.Author, "matched_keyword", matchedKeyword)
}

func cleanHTML(s string) string {
	res := htmlRegex.ReplaceAllString(s, "")
	res = strings.ReplaceAll(res, "&quot;", "\"")
	res = strings.ReplaceAll(res, "&#x27;", "'")
	res = strings.ReplaceAll(res, "&gt;", ">")
	res = strings.ReplaceAll(res, "&lt;", "<")
	return strings.TrimSpace(res)
}
