// Package ranking implements Stage 2 of the pipeline: scoring. Every candidate
// from retrieval gets a single relevance score from a transparent weighted-sum
// over interpretable features (see docs/07 and docs/01 D7). The formula is
// shaped exactly like a trained model's input (a feature vector per candidate),
// so swapping in an XGBoost/neural scorer later means replacing Score() only.
package ranking

import (
	"math"
	"sort"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
)

// Weights are the tunable coefficients of the ranking formula. Exposed so they
// can be configured and A/B-tested; defaults are hand-chosen sane values.
type Weights struct {
	EmbeddingSimilarity float64            // how close to the user's taste vector
	Popularity          float64            // log-scaled trending score
	Recency             float64            // exponential recency bonus
	PriceMatch          float64            // gaussian fit to the user's price zone
	CategoryMatch       float64            // flat bonus if in a preferred category
	Rating              float64            // objective item quality
	SourceBoost         map[string]float64 // multiplicative boost by retrieval source
}

// DefaultWeights returns a balanced starting point.
func DefaultWeights() Weights {
	return Weights{
		EmbeddingSimilarity: 1.0,
		Popularity:          0.3,
		Recency:             0.4,
		PriceMatch:          0.5,
		CategoryMatch:       0.6,
		Rating:              0.3,
		SourceBoost: map[string]float64{
			"embedding": 1.10, // trust content match a bit more
			"collab":    1.05, // collaborative signal is valuable
			"trending":  1.00,
			"fresh":     0.95, // exploration, slightly discounted
		},
	}
}

// Ranker scores candidates with a fixed set of weights.
type Ranker struct {
	w Weights
}

// New builds a Ranker.
func New(w Weights) *Ranker { return &Ranker{w: w} }

// Score computes the relevance of one candidate for one user. Each term is
// commented with the intuition; all are combined additively, then scaled by the
// candidate's source boost.
func (r *Ranker) Score(p models.UserProfile, c models.Candidate) float64 {
	w := r.w
	var s float64

	// Embedding similarity: how close the item is to the user's taste vector.
	s += w.EmbeddingSimilarity * c.SimilarityScore

	// Popularity: PopularityScore arrives normalized to [0,1] within the request's
	// candidate pool (the recommender rescales the raw, arbitrarily-large trending
	// scores). log1p keeps the diminishing-returns shape on that [0,1] range.
	s += w.Popularity * math.Log1p(c.PopularityScore)

	// Recency: exponential decay in days-since-created; newer scores higher.
	days := time.Since(c.Item.CreatedAt).Hours() / 24
	s += w.Recency * math.Exp(-0.1*days)

	// Price match: gaussian around the user's typical spend. A user with erratic
	// spending (high std) is penalized less for a mismatch. Skipped for users
	// with no price history (std == 0).
	if p.PriceStd > 0 {
		z := (c.Item.Price - p.AvgPrice) / p.PriceStd
		s += w.PriceMatch * math.Exp(-0.5*z*z)
	}

	// Category match: flat bonus if the item's category is one the user prefers.
	if contains(p.PreferredCategories, c.Item.Category) {
		s += w.CategoryMatch
	}

	// Objective quality.
	s += w.Rating * (c.Item.Rating / 5.0)

	// Source boost (multiplicative): trust some retrieval sources more.
	if b, ok := w.SourceBoost[c.Source]; ok {
		s *= b
	}
	return s
}

// Rank scores every candidate and returns them sorted by descending score.
func (r *Ranker) Rank(p models.UserProfile, cands []models.Candidate) []models.ScoredItem {
	out := make([]models.ScoredItem, 0, len(cands))
	for _, c := range cands {
		out = append(out, models.ScoredItem{
			Item:      c.Item,
			Source:    c.Source,
			Score:     r.Score(p, c),
			Embedding: c.Embedding,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
