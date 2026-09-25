// Package profile turns a single event into all the derived-state updates the
// online system needs. It is the heart of the streaming write path: the
// processor hands it each event, and it fans the update out to Redis (and
// appends to the durable interaction log).
//
// The updates it performs per event (see docs/05):
//   - append to recently-viewed
//   - bump the item's category counter (weighted by signal strength)
//   - fold the item's price into the running mean/std
//   - fold the item's embedding into the user's weighted running-average vector
//   - increment co-occurrence between this item and the user's recent items
//   - add a time-decayed weight to trending
//   - append the raw event to Postgres (durable history)
package profile

import (
	"context"

	"github.com/1morshed1/reco-engine/internal/embedding"
	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
)

// Updater applies event-driven updates. It holds in-memory item metadata and
// embeddings so per-event handling needs no DB round-trip for item lookups —
// fine at this scale; a production system would use a feature store / cache
// (see docs/05).
type Updater struct {
	redis   *redisstore.Store
	pg      *postgres.Store
	items   map[int64]models.Item
	embs    map[int64][]float32
	dim     int
	recentN int // how many recent items to keep and to co-occur against
}

// Config tunes the updater.
type Config struct {
	EmbeddingDim int
	RecentItems  int // list length + co-occurrence window
}

// New builds an Updater, loading the full catalog into memory for fast lookups.
func New(ctx context.Context, rs *redisstore.Store, pg *postgres.Store, cfg Config) (*Updater, error) {
	items, embs, err := pg.AllItems(ctx)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]models.Item, len(items))
	for _, it := range items {
		byID[it.ID] = it
	}
	recentN := cfg.RecentItems
	if recentN <= 0 {
		recentN = 20
	}
	return &Updater{redis: rs, pg: pg, items: byID, embs: embs, dim: cfg.EmbeddingDim, recentN: recentN}, nil
}

// Handle applies all derived-state updates for one event.
func (u *Updater) Handle(ctx context.Context, ev models.Event) error {
	// Always append to the durable log, even for unknown items.
	if err := u.pg.InsertInteraction(ctx, ev); err != nil {
		return err
	}

	item, ok := u.items[ev.ItemID]
	if !ok {
		// Unknown item: logged, but no derived updates possible.
		return nil
	}
	w := ev.Type.Weight()

	// Co-occurrence FIRST, against the user's current recent items (before we add
	// this one), so we don't co-occur an item with itself.
	recent, err := u.redis.GetProfile(ctx, ev.UserID, u.recentN, 0)
	if err != nil {
		return err
	}
	for _, prev := range recent.RecentlyViewed {
		if err := u.redis.IncrCoOccurrence(ctx, ev.ItemID, prev, w); err != nil {
			return err
		}
	}

	// Recently viewed + counters.
	if err := u.redis.PushRecent(ctx, ev.UserID, ev.ItemID, u.recentN); err != nil {
		return err
	}
	if err := u.redis.IncrCategory(ctx, ev.UserID, item.Category, w); err != nil {
		return err
	}
	if err := u.redis.AddPrice(ctx, ev.UserID, item.Price); err != nil {
		return err
	}
	if err := u.redis.IncrInteractions(ctx, ev.UserID); err != nil {
		return err
	}

	// Weighted running-average embedding update.
	if err := u.updateEmbedding(ctx, ev.UserID, ev.ItemID, w); err != nil {
		return err
	}

	// Trending (time-decayed).
	return u.redis.AddTrending(ctx, ev.ItemID, item.Category, w, ev.Timestamp)
}

// updateEmbedding folds the item's embedding into the user's weighted running
// average: new = (old*W + itemEmb*w) / (W + w), then re-normalized. W is the
// accumulated weight stored alongside the vector.
func (u *Updater) updateEmbedding(ctx context.Context, userID, itemID int64, w float64) error {
	itemEmb := u.embs[itemID]
	if len(itemEmb) == 0 {
		return nil
	}
	old, err := u.redis.GetEmbedding(ctx, userID)
	if err != nil {
		return err
	}
	W, err := u.redis.EmbWeight(ctx, userID)
	if err != nil {
		return err
	}
	dim := len(itemEmb)
	next := make([]float32, dim)
	if len(old) == dim && W > 0 {
		total := W + w
		for i := range next {
			next[i] = float32((float64(old[i])*W + float64(itemEmb[i])*w) / total)
		}
	} else {
		copy(next, itemEmb)
	}
	embedding.Normalize(next)

	if err := u.redis.SetEmbedding(ctx, userID, next); err != nil {
		return err
	}
	return u.redis.SetEmbWeight(ctx, userID, W+w)
}
