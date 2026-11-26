package dataset

import (
	"testing"
)

func small() Params {
	return Params{NumItems: 100, NumUsers: 20, EventsPerUser: 10, Seed: 7, HistoryDays: 30}
}

// The same seed must produce the same world, so eval runs are reproducible.
func TestCatalogDeterministic(t *testing.T) {
	a, b := Catalog(small()), Catalog(small())
	if len(a) != 100 || len(b) != 100 {
		t.Fatalf("expected 100 items, got %d / %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Title != b[i].Title || a[i].Category != b[i].Category {
			t.Fatalf("item %d differs between runs with the same seed", i)
		}
	}
}

func TestCatalogFieldsValid(t *testing.T) {
	for _, it := range Catalog(small()) {
		if len(it.Genres) < 1 || len(it.Genres) > 3 {
			t.Fatalf("item %d: %d genres, want 1..3", it.ID, len(it.Genres))
		}
		if !contains(it.Genres, it.Category) {
			t.Fatalf("item %d: category %q not among genres %v", it.ID, it.Category, it.Genres)
		}
		if it.Rating < 0 || it.Rating > 5 {
			t.Fatalf("item %d: rating %v out of range", it.ID, it.Rating)
		}
		if it.Price <= 0 {
			t.Fatalf("item %d: non-positive price %v", it.ID, it.Price)
		}
	}
}

// Events are sorted by time and reference only real users and items.
func TestGenerateInteractions(t *testing.T) {
	p := small()
	cat := Catalog(p)
	users := Users(p)
	evs := GenerateInteractions(p, cat, users)
	if len(evs) == 0 {
		t.Fatal("no events generated")
	}
	itemIDs := map[int64]bool{}
	for _, it := range cat {
		itemIDs[it.ID] = true
	}
	for i, e := range evs {
		if !itemIDs[e.ItemID] {
			t.Fatalf("event %d references unknown item %d", i, e.ItemID)
		}
		if i > 0 && e.Timestamp.Before(evs[i-1].Timestamp) {
			t.Fatalf("events not sorted by time at %d", i)
		}
	}
}

func TestEmbedCoversCatalog(t *testing.T) {
	cat := Catalog(small())
	embs := Embed(cat, 32)
	if len(embs) != len(cat) {
		t.Fatalf("expected an embedding per item: %d vs %d", len(embs), len(cat))
	}
	for id, v := range embs {
		if len(v) != 32 {
			t.Fatalf("item %d: dim %d, want 32", id, len(v))
		}
	}
}
