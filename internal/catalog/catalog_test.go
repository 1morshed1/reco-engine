package catalog

import (
	"testing"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
)

// newTestStore builds a Store directly from items, bypassing Postgres.
func newTestStore(items []models.Item) *Store {
	s := &Store{byID: map[int64]models.Candidate{}, itemByID: map[int64]models.Item{}}
	for _, it := range items {
		s.byID[it.ID] = models.Candidate{Item: it, Embedding: []float32{float32(it.ID)}}
		s.itemByID[it.ID] = it
		s.byCreated = append(s.byCreated, it.ID)
	}
	// Caller passes items newest-first, matching Load's sort.
	return s
}

func day(n int) time.Time { return time.Now().Add(-time.Duration(n) * 24 * time.Hour) }

func TestByIDsLabelsAndSkipsUnknown(t *testing.T) {
	s := newTestStore([]models.Item{{ID: 1, CreatedAt: day(1)}, {ID: 2, CreatedAt: day(2)}})
	out := s.ByIDs([]int64{2, 99, 1}, "collab")
	if len(out) != 2 {
		t.Fatalf("unknown id should be skipped: got %d", len(out))
	}
	if out[0].Item.ID != 2 || out[1].Item.ID != 1 {
		t.Fatalf("input order should be preserved")
	}
	for _, c := range out {
		if c.Source != "collab" {
			t.Fatalf("source label not applied: %q", c.Source)
		}
		if len(c.Embedding) == 0 {
			t.Fatalf("embedding should be carried")
		}
	}
}

func TestFreshWindowCategoryAndLimit(t *testing.T) {
	s := newTestStore([]models.Item{
		{ID: 1, Category: "SciFi", CreatedAt: day(1)},
		{ID: 2, Category: "Drama", CreatedAt: day(2)},
		{ID: 3, Category: "SciFi", CreatedAt: day(3)},
		{ID: 4, Category: "SciFi", CreatedAt: day(40)}, // outside window
	})

	all := s.Fresh(nil, 10, 30)
	if len(all) != 3 {
		t.Fatalf("expected 3 items inside 30-day window, got %d", len(all))
	}

	scifi := s.Fresh([]string{"SciFi"}, 10, 30)
	if len(scifi) != 2 || scifi[0].Item.ID != 1 || scifi[1].Item.ID != 3 {
		t.Fatalf("category filter / newest-first order wrong: %+v", scifi)
	}
	for _, c := range scifi {
		if c.Source != "fresh" {
			t.Fatalf("source should be fresh")
		}
	}

	if got := s.Fresh(nil, 1, 30); len(got) != 1 || got[0].Item.ID != 1 {
		t.Fatalf("limit k=1 should return newest item only")
	}
}

func TestSize(t *testing.T) {
	if n := newTestStore([]models.Item{{ID: 1}, {ID: 2}}).Size(); n != 2 {
		t.Fatalf("size: got %d", n)
	}
}
