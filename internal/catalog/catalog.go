// Package catalog is an in-memory, read-only cache of the item catalog for the
// serving path. It exists to keep the recommendation request off Postgres for
// everything except the ANN search.
//
// Why (see docs/11): retrieval needs to (a) hydrate item IDs from co-occurrence
// and trending into full records, and (b) list fresh items. Doing those as SQL
// on every request means 2+ extra DB round-trips per recommendation — the
// dominant latency cost under load. The catalog rarely changes, so we load it
// once into memory and serve those two operations locally. Only ANN (which needs
// the pgvector index) still touches Postgres.
//
// This is the same idea as a feature store's online cache: precomputed, static-
// ish data served from RAM on the hot path.
package catalog

import (
	"context"
	"sort"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
)

// Store is an immutable snapshot of the catalog. Safe for concurrent reads.
type Store struct {
	byID      map[int64]models.Candidate // id -> candidate (item + embedding)
	byCreated []int64                    // item IDs sorted by created_at DESC (for Fresh)
	itemByID  map[int64]models.Item
}

// Load builds the snapshot from Postgres. Call once at startup (and again from a
// refresh loop if the catalog changes).
func Load(ctx context.Context, pg *postgres.Store) (*Store, error) {
	items, embs, err := pg.AllItems(ctx)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]models.Candidate, len(items))
	itemByID := make(map[int64]models.Item, len(items))
	ids := make([]int64, 0, len(items))
	for _, it := range items {
		byID[it.ID] = models.Candidate{Item: it, Embedding: embs[it.ID]}
		itemByID[it.ID] = it
		ids = append(ids, it.ID)
	}
	// Sort IDs by created_at descending so Fresh is a simple prefix scan.
	sort.Slice(ids, func(i, j int) bool {
		return itemByID[ids[i]].CreatedAt.After(itemByID[ids[j]].CreatedAt)
	})
	return &Store{byID: byID, byCreated: ids, itemByID: itemByID}, nil
}

// Size returns the number of items cached.
func (s *Store) Size() int { return len(s.byID) }

// ByIDs turns item IDs into candidates with the given source label, preserving
// input order and skipping unknown IDs. Replaces a Postgres hydrate query.
func (s *Store) ByIDs(ids []int64, source string) []models.Candidate {
	out := make([]models.Candidate, 0, len(ids))
	for _, id := range ids {
		if c, ok := s.byID[id]; ok {
			c.Source = source
			out = append(out, c)
		}
	}
	return out
}

// Fresh returns up to k items created within the last `days`, optionally filtered
// to the given categories, newest first. Replaces a Postgres recency query.
func (s *Store) Fresh(categories []string, k, days int) []models.Candidate {
	cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	catSet := map[string]struct{}{}
	for _, c := range categories {
		catSet[c] = struct{}{}
	}
	out := make([]models.Candidate, 0, k)
	for _, id := range s.byCreated {
		it := s.itemByID[id]
		if it.CreatedAt.Before(cutoff) {
			break // list is sorted desc by created_at, so we're past the window
		}
		if len(catSet) > 0 {
			if _, ok := catSet[it.Category]; !ok {
				continue
			}
		}
		c := s.byID[id]
		c.Source = "fresh"
		out = append(out, c)
		if len(out) == k {
			break
		}
	}
	return out
}
