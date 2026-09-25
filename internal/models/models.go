// Package models defines the domain types shared across the engine.
//
// These types are deliberately plain structs with no behavior — they are the
// vocabulary the whole system speaks (events, items, profiles, candidates).
// Keeping them dependency-free means every layer (stores, retrieval, ranking)
// can import them without import cycles.
package models

import "time"

// EventType enumerates the kinds of user activity we ingest. The ordering
// loosely reflects intent strength: a purchase is a stronger signal than a view.
type EventType string

const (
	EventView     EventType = "view"     // user looked at an item
	EventClick    EventType = "click"    // user clicked through to an item
	EventPurchase EventType = "purchase" // user bought an item (strongest signal)
	EventRating   EventType = "rating"   // user rated an item; Value holds the score
)

// Weight returns how strongly this event type should move a user profile.
// Purchases and high ratings say the most about taste; a passing view says the
// least. These weights are used when we update preference counters in Redis.
func (e EventType) Weight() float64 {
	switch e {
	case EventPurchase:
		return 3.0
	case EventRating:
		return 2.0
	case EventClick:
		return 1.5
	case EventView:
		return 1.0
	default:
		return 0.5
	}
}

// Event is a single user activity record. It is what flows through Kafka and
// what the processor consumes.
type Event struct {
	UserID    int64     `json:"user_id"`
	ItemID    int64     `json:"item_id"`
	Type      EventType `json:"type"`
	Value     float64   `json:"value,omitempty"` // e.g. rating 1..5; 0 for non-rating events
	Timestamp time.Time `json:"timestamp"`
}

// Item is a catalog entry (a movie, in our dataset). This is the durable
// source-of-truth record stored in Postgres.
type Item struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Category  string    `json:"category"` // primary genre, used for fast category matching
	Genres    []string  `json:"genres"`   // all genres; drives the content embedding
	Year      int       `json:"year"`
	Price     float64   `json:"price"`  // synthetic "price" so we can demo price-match ranking
	Rating    float64   `json:"rating"` // average catalog rating 0..5
	CreatedAt time.Time `json:"created_at"`
}

// UserProfile is the live, derived view of a user, kept hot in Redis and read on
// every recommendation request. It is rebuilt from the event stream, never
// authored directly.
type UserProfile struct {
	UserID              int64          `json:"user_id"`
	RecentlyViewed      []int64        `json:"recently_viewed"`      // most-recent-first item IDs
	PreferredCategories []string       `json:"preferred_categories"` // top categories by weighted count
	CategoryCounts      map[string]float64 `json:"category_counts"`  // weighted interaction count per category
	AvgPrice            float64        `json:"avg_price"`            // mean price of items interacted with
	PriceStd            float64        `json:"price_std"`            // std dev of those prices (for price-match)
	Embedding           []float32      `json:"embedding"`            // running average of interacted item embeddings
	InteractionCount    int64          `json:"interaction_count"`
}

// Candidate is an item proposed by a retrieval source, carrying the metadata the
// ranker needs. SimilarityScore is meaningful only for the embedding source;
// other sources leave it at 0 and rely on their own signals.
type Candidate struct {
	Item            Item
	Source          string    // which retrieval source produced it: "embedding" | "collab" | "trending" | "fresh"
	SimilarityScore float64   // cosine similarity to the user embedding (embedding source only)
	PopularityScore float64   // trending score at retrieval time
	Embedding       []float32 // item embedding, carried through for MMR diversity
}

// ScoredItem is a candidate after ranking. Score is the ranker output; the
// embedding is retained so the diversity stage can measure novelty.
type ScoredItem struct {
	Item      Item
	Source    string
	Score     float64
	Embedding []float32
}
