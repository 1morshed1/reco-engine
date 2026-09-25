// Package diversity implements MMR (Maximal Marginal Relevance) re-ranking, the
// post-processing step that stops recommendations from collapsing onto a set of
// near-duplicates (five movies from the same franchise). See docs/07.
//
// MMR builds the result greedily. It always takes the top-ranked item first,
// then repeatedly picks the item that maximizes:
//
//	mmr = lambda * relevance  -  (1 - lambda) * maxSimilarityToAlreadyChosen
//
// lambda near 1 => pure relevance (ranking order). lambda near 0 => pure novelty.
// A middling lambda (~0.7) keeps strong items while spreading variety.
package diversity

import (
	"math"

	"github.com/1morshed1/reco-engine/internal/embedding"
	"github.com/1morshed1/reco-engine/internal/models"
)

// MMR re-ranks `ranked` (already sorted by relevance, descending) into a diverse
// top-N. `lambda` in [0,1] trades relevance against novelty. Novelty is measured
// as cosine similarity in embedding space to the items already selected.
//
// Relevance scores are normalized to [0,1] first so they're on the same scale as
// the [0,1]-ish similarity term — otherwise a raw score of, say, 3.0 would swamp
// the similarity penalty and lambda would do nothing.
func MMR(ranked []models.ScoredItem, topN int, lambda float64) []models.ScoredItem {
	if len(ranked) == 0 || topN <= 0 {
		return nil
	}
	if topN >= len(ranked) {
		topN = len(ranked)
	}

	rel := normalizedScores(ranked)

	selected := make([]models.ScoredItem, 0, topN)
	selectedIdx := make([]int, 0, topN)
	chosen := make([]bool, len(ranked))

	// Seed with the top-ranked item.
	selected = append(selected, ranked[0])
	selectedIdx = append(selectedIdx, 0)
	chosen[0] = true

	for len(selected) < topN {
		bestIdx := -1
		bestScore := math.Inf(-1)
		for i := range ranked {
			if chosen[i] {
				continue
			}
			// Redundancy: max similarity to anything already picked.
			maxSim := 0.0
			for _, si := range selectedIdx {
				sim := embedding.Cosine(ranked[i].Embedding, ranked[si].Embedding)
				if sim > maxSim {
					maxSim = sim
				}
			}
			mmr := lambda*rel[i] - (1-lambda)*maxSim
			if mmr > bestScore {
				bestScore = mmr
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		selected = append(selected, ranked[bestIdx])
		selectedIdx = append(selectedIdx, bestIdx)
		chosen[bestIdx] = true
	}
	return selected
}

// normalizedScores maps the relevance scores to [0,1] via min-max scaling so the
// MMR relevance and novelty terms are comparable. A flat set maps to all 1s.
func normalizedScores(items []models.ScoredItem) []float64 {
	min, max := math.Inf(1), math.Inf(-1)
	for _, it := range items {
		if it.Score < min {
			min = it.Score
		}
		if it.Score > max {
			max = it.Score
		}
	}
	out := make([]float64, len(items))
	span := max - min
	for i, it := range items {
		if span <= 0 {
			out[i] = 1
		} else {
			out[i] = (it.Score - min) / span
		}
	}
	return out
}
