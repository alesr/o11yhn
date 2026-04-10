package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/alesr/o11yhn/internal/engine"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type ClickHouse struct{ conn clickhouse.Conn }

var tracer = otel.Tracer("github.com/alesr/o11yhn/internal/storage")

func NewClickHouse(addr, user, password, database string) (*ClickHouse, error) {
	opts := clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
	}

	conn, err := clickhouse.Open(&opts)
	if err != nil {
		return nil, fmt.Errorf("could not connect to clickhouse: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

func (ch *ClickHouse) EnsureSchema(ctx context.Context) error {
	if err := ch.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS mentions (
			id UInt64,
			type String,
			matched_keyword String,
			matched_keywords Array(String),
			title String,
			text String,
			author String,
			url String,
			created_at DateTime,
			ingested_at DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = MergeTree
		ORDER BY (created_at, id)
	`); err != nil {
		return fmt.Errorf("could not ensure mentions table: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		ALTER TABLE mentions
		ADD COLUMN IF NOT EXISTS matched_keywords Array(String)
		AFTER matched_keyword
	`); err != nil {
		return fmt.Errorf("could not ensure matched_keywords column: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		ALTER TABLE mentions
		ADD COLUMN IF NOT EXISTS ingested_at DateTime64(3) DEFAULT now64(3)
		AFTER created_at
	`); err != nil {
		return fmt.Errorf("could not ensure ingested_at column: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS mentions_canonical (
			id UInt64,
			type String,
			matched_keyword String,
			matched_keywords Array(String),
			title String,
			text String,
			author String,
			url String,
			created_at DateTime,
			ingested_at DateTime64(3)
		)
		ENGINE = ReplacingMergeTree(ingested_at)
		ORDER BY id
	`); err != nil {
		return fmt.Errorf("could not ensure mentions_canonical table: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS mentions_to_canonical_mv
		TO mentions_canonical
		AS
		SELECT
			id,
			type,
			matched_keyword,
			matched_keywords,
			title,
			text,
			author,
			url,
			created_at,
			ingested_at
		FROM mentions
	`); err != nil {
		return fmt.Errorf("could not ensure mentions_to_canonical_mv: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		INSERT INTO mentions_canonical
		SELECT
			id,
			type,
			matched_keyword,
			matched_keywords,
			title,
			text,
			author,
			url,
			created_at,
			latest_ingested_at AS ingested_at
		FROM (
			SELECT
				id,
				argMax(type, ingested_at) AS type,
				argMax(matched_keyword, ingested_at) AS matched_keyword,
				argMax(matched_keywords, ingested_at) AS matched_keywords,
				argMax(title, ingested_at) AS title,
				argMax(text, ingested_at) AS text,
				argMax(author, ingested_at) AS author,
				argMax(url, ingested_at) AS url,
				argMax(created_at, ingested_at) AS created_at,
				max(ingested_at) AS latest_ingested_at
			FROM mentions
			WHERE (SELECT count() FROM mentions_canonical) = 0
			GROUP BY id
		)
	`); err != nil {
		return fmt.Errorf("could not backfill mentions_canonical table: %w", err)
	}

	if err := ch.conn.Exec(ctx, `
		CREATE OR REPLACE VIEW mentions_latest AS
		SELECT
			id,
			type,
			matched_keyword,
			matched_keywords,
			title,
			text,
			author,
			url,
			created_at,
			ingested_at
		FROM mentions_canonical FINAL
	`); err != nil {
		return fmt.Errorf("could not ensure mentions_latest view: %w", err)
	}
	return nil
}

func (ch *ClickHouse) BatchInsert(ctx context.Context, mentions []engine.HNMention) error {
	ctx, span := tracer.Start(ctx, "clickhouse.batch_insert")
	defer span.End()

	span.SetAttributes(attribute.Int("clickhouse.batch_size", len(mentions)))

	batch, err := ch.conn.PrepareBatch(ctx, `
		INSERT INTO mentions (
			id,
			type,
			matched_keyword,
			matched_keywords,
			title,
			text,
			author,
			url,
			created_at,
			ingested_at
		)
	`)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prepare batch failed")
		return fmt.Errorf("could not prepare batch: %w", err)
	}

	for _, m := range mentions {
		ingestedAt := time.Now().UTC()

		if err := batch.Append(
			m.ID,
			m.Type,
			m.MatchedKeyword,
			m.MatchedKeywords,
			m.Title,
			m.Text,
			m.Author,
			m.URL,
			m.CreatedAt,
			ingestedAt,
		); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "append batch failed")
			return fmt.Errorf("could not append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "send batch failed")
		return fmt.Errorf("could not send batch: %w", err)
	}
	return nil
}
