package embedding

import (
	"math"
	"testing"

	"github.com/1morshed1/reco-engine/internal/models"
)

// TestItemVectorSimilarity checks the core property the whole system relies on:
// items sharing genres are more similar than items that don't.
func TestItemVectorSimilarity(t *testing.T) {
	dim := 32
	sciFiA := ItemVector(models.Item{Genres: []string{"SciFi", "Action"}, Year: 2010, Rating: 4}, dim)
	sciFiB := ItemVector(models.Item{Genres: []string{"SciFi", "Thriller"}, Year: 2012, Rating: 4}, dim)
	romance := ItemVector(models.Item{Genres: []string{"Romance", "Musical"}, Year: 2011, Rating: 4}, dim)

	simSame := Cosine(sciFiA, sciFiB)
	simDiff := Cosine(sciFiA, romance)
	if simSame <= simDiff {
		t.Fatalf("expected shared-genre similarity (%.3f) > cross-genre (%.3f)", simSame, simDiff)
	}
}

// TestNormalizeUnitLength verifies vectors come out unit length.
func TestNormalizeUnitLength(t *testing.T) {
	v := ItemVector(models.Item{Genres: []string{"Drama"}, Year: 2000, Rating: 3}, 32)
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if math.Abs(math.Sqrt(sum)-1) > 1e-5 {
		t.Fatalf("expected unit length, got %.6f", math.Sqrt(sum))
	}
}

// TestCosineBounds checks identical vectors give ~1 and length mismatch gives 0.
func TestCosineBounds(t *testing.T) {
	v := ItemVector(models.Item{Genres: []string{"Horror"}, Year: 1999, Rating: 3}, 32)
	if s := Cosine(v, v); math.Abs(s-1) > 1e-6 {
		t.Fatalf("self-similarity should be 1, got %.6f", s)
	}
	if s := Cosine(v, []float32{1, 2, 3}); s != 0 {
		t.Fatalf("mismatched lengths should be 0, got %.6f", s)
	}
}

// TestAverageDirection checks the average of two vectors points between them.
func TestAverageDirection(t *testing.T) {
	a := ItemVector(models.Item{Genres: []string{"SciFi"}, Year: 2010, Rating: 4}, 32)
	b := ItemVector(models.Item{Genres: []string{"Comedy"}, Year: 2010, Rating: 4}, 32)
	avg := Average([][]float32{a, b}, 32)
	if Cosine(avg, a) <= 0 || Cosine(avg, b) <= 0 {
		t.Fatalf("average should be positively similar to both inputs")
	}
}
