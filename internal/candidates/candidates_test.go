package candidates

import (
	"sort"
	"testing"

	"github.com/1morshed1/reco-engine/internal/models"
)

// An item returned by several sources collapses to one candidate that keeps
// its first source but inherits the best similarity and any embedding.
func TestDeduplicateMergesSignals(t *testing.T) {
	in := []models.Candidate{
		{Item: models.Item{ID: 1}, Source: "trending"},
		{Item: models.Item{ID: 2}, Source: "collab"},
		{Item: models.Item{ID: 1}, Source: "embedding", SimilarityScore: 0.8, Embedding: []float32{1, 2}},
		{Item: models.Item{ID: 1}, Source: "fresh", SimilarityScore: 0.3},
	}
	out := deduplicate(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique candidates, got %d", len(out))
	}
	first := out[0]
	if first.Item.ID != 1 || first.Source != "trending" {
		t.Fatalf("first occurrence should be kept: got id=%d source=%s", first.Item.ID, first.Source)
	}
	if first.SimilarityScore != 0.8 {
		t.Fatalf("best similarity should be carried over: got %v", first.SimilarityScore)
	}
	if len(first.Embedding) != 2 {
		t.Fatalf("embedding should be carried over from a later source")
	}
}

// Order of first appearance is preserved.
func TestDeduplicatePreservesOrder(t *testing.T) {
	in := []models.Candidate{
		{Item: models.Item{ID: 3}}, {Item: models.Item{ID: 1}}, {Item: models.Item{ID: 3}}, {Item: models.Item{ID: 2}},
	}
	out := deduplicate(in)
	want := []int64{3, 1, 2}
	for i, c := range out {
		if c.Item.ID != want[i] {
			t.Fatalf("position %d: got %d want %d", i, c.Item.ID, want[i])
		}
	}
}

func TestDeduplicateEmpty(t *testing.T) {
	if out := deduplicate(nil); len(out) != 0 {
		t.Fatalf("expected empty, got %d", len(out))
	}
}

func TestKeys(t *testing.T) {
	got := keys(map[int64]struct{}{5: {}, 1: {}, 3: {}})
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Fatalf("unexpected keys: %v", got)
	}
}
