package diversity

import (
	"testing"

	"github.com/1morshed1/reco-engine/internal/models"
)

// twoClusters builds 4 items: two near-duplicate pairs. A pure-relevance top-2
// would pick the two highest scores (which are in the SAME cluster); MMR with a
// low lambda should instead spread across clusters.
func twoClusters() []models.ScoredItem {
	clusterA := []float32{1, 0}
	clusterB := []float32{0, 1}
	return []models.ScoredItem{
		{Item: models.Item{ID: 1}, Score: 1.00, Embedding: clusterA},
		{Item: models.Item{ID: 2}, Score: 0.95, Embedding: clusterA}, // near-dup of 1
		{Item: models.Item{ID: 3}, Score: 0.90, Embedding: clusterB},
		{Item: models.Item{ID: 4}, Score: 0.85, Embedding: clusterB}, // near-dup of 3
	}
}

// TestMMRDiversifies: with low lambda, the second pick should jump to the other
// cluster (item 3), not the near-duplicate (item 2).
func TestMMRDiversifies(t *testing.T) {
	out := MMR(twoClusters(), 2, 0.2)
	if len(out) != 2 {
		t.Fatalf("expected 2 items, got %d", len(out))
	}
	if out[0].Item.ID != 1 {
		t.Fatalf("top item should always be the highest-ranked (1), got %d", out[0].Item.ID)
	}
	if out[1].Item.ID != 3 {
		t.Fatalf("with low lambda the 2nd pick should be the diverse item 3, got %d", out[1].Item.ID)
	}
}

// TestMMRHighLambdaKeepsRelevance: with lambda=1 it's pure relevance order.
func TestMMRHighLambdaKeepsRelevance(t *testing.T) {
	out := MMR(twoClusters(), 2, 1.0)
	if out[0].Item.ID != 1 || out[1].Item.ID != 2 {
		t.Fatalf("lambda=1 should give pure relevance order [1,2], got [%d,%d]",
			out[0].Item.ID, out[1].Item.ID)
	}
}

// TestMMREmpty handles the empty input gracefully.
func TestMMREmpty(t *testing.T) {
	if out := MMR(nil, 5, 0.5); out != nil {
		t.Fatalf("expected nil for empty input, got %v", out)
	}
}
