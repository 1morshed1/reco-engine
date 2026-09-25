package ranking

import (
	"testing"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
)

// TestCategoryMatchBoost: an item in a preferred category should outscore an
// otherwise-identical item that isn't.
func TestCategoryMatchBoost(t *testing.T) {
	r := New(DefaultWeights())
	p := models.UserProfile{PreferredCategories: []string{"SciFi"}}
	base := models.Candidate{Item: models.Item{Category: "Drama", Rating: 3, CreatedAt: time.Now()}, Source: "trending"}
	pref := models.Candidate{Item: models.Item{Category: "SciFi", Rating: 3, CreatedAt: time.Now()}, Source: "trending"}
	if r.Score(p, pref) <= r.Score(p, base) {
		t.Fatalf("preferred-category item should score higher")
	}
}

// TestSimilarityDominates: higher embedding similarity should raise the score.
func TestSimilarityDominates(t *testing.T) {
	r := New(DefaultWeights())
	p := models.UserProfile{}
	low := models.Candidate{Item: models.Item{CreatedAt: time.Now()}, SimilarityScore: 0.1, Source: "embedding"}
	high := models.Candidate{Item: models.Item{CreatedAt: time.Now()}, SimilarityScore: 0.9, Source: "embedding"}
	if r.Score(p, high) <= r.Score(p, low) {
		t.Fatalf("higher similarity should score higher")
	}
}

// TestRankSorts: Rank returns items in descending score order.
func TestRankSorts(t *testing.T) {
	r := New(DefaultWeights())
	p := models.UserProfile{}
	cands := []models.Candidate{
		{Item: models.Item{ID: 1, CreatedAt: time.Now()}, SimilarityScore: 0.2, Source: "embedding"},
		{Item: models.Item{ID: 2, CreatedAt: time.Now()}, SimilarityScore: 0.8, Source: "embedding"},
		{Item: models.Item{ID: 3, CreatedAt: time.Now()}, SimilarityScore: 0.5, Source: "embedding"},
	}
	out := r.Rank(p, cands)
	for i := 1; i < len(out); i++ {
		if out[i-1].Score < out[i].Score {
			t.Fatalf("ranking not sorted descending at %d", i)
		}
	}
	if out[0].Item.ID != 2 {
		t.Fatalf("expected highest-similarity item (2) first, got %d", out[0].Item.ID)
	}
}
