package engine

import "time"

// HNMention represents a verified, cleaned mention of a keyword
// This is the "Domain Model" used by our Kafka Producer and ClickHouse Consumer.
type HNMention struct {
	ID              int       `json:"id"`
	Type            string    `json:"type"`
	MatchedKeyword  string    `json:"matched_keyword"`
	MatchedKeywords []string  `json:"matched_keywords"`
	Title           string    `json:"title"`
	Text            string    `json:"text"`
	Author          string    `json:"author"`
	URL             string    `json:"url"`
	CreatedAt       time.Time `json:"created_at"`
}
