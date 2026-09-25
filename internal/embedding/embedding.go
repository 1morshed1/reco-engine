// Package embedding turns item content into vectors and provides the vector
// math (cosine similarity, averaging) the rest of the engine relies on.
//
// Design (see docs/03 and docs/01 D5): embeddings are *content-based* and
// computed deterministically in-process — no external model or API. An item's
// vector is built from its genres (multi-hot) plus a couple of normalized scalar
// features (recency, rating). Two items that share genres land near each other,
// so cosine similarity and ANN search behave meaningfully, and the whole system
// runs offline. Swapping in learned embeddings later means replacing only
// ItemVector — nothing downstream changes.
package embedding

import (
	"math"

	"github.com/1morshed1/reco-engine/internal/models"
)

// Genres is the fixed vocabulary (the MovieLens genre set). Each genre owns one
// dimension of the embedding, so the vector must have at least len(Genres)+Extra
// dimensions. Order is fixed forever: changing it would invalidate stored
// vectors.
var Genres = []string{
	"Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
	"Documentary", "Drama", "Fantasy", "Horror", "Musical", "Mystery",
	"Romance", "SciFi", "Thriller", "War", "Western", "IMAX",
}

// extraDims are the non-genre feature slots appended after the genre block:
// [recency, rating]. Total dimension = len(Genres) + extraDims.
const extraDims = 2

// Dim is the natural content dimensionality (genres + extra features). Vectors
// are padded to config.EmbeddingDim (the DB column width) when built, so this is
// informational; it's a var, not a const, because len() of a slice isn't const.
var Dim = len(Genres) + extraDims // 18 + 2 = 20

// genreIndex maps a genre name to its dimension for O(1) lookup.
var genreIndex = func() map[string]int {
	m := make(map[string]int, len(Genres))
	for i, g := range Genres {
		m[g] = i
	}
	return m
}()

// ItemVector builds an item's content embedding.
//
// Construction:
//   - dims [0..len(Genres))   : multi-hot genre membership (1.0 per genre)
//   - dim  len(Genres)+0      : recency, normalized to ~[0,1] over 1970..now
//   - dim  len(Genres)+1      : rating, normalized to [0,1] over 0..5
//
// The full vector is L2-normalized so cosine similarity reduces to a dot
// product and the ANN index (vector_cosine_ops) is well-behaved.
//
// The returned slice is padded/truncated to targetDim so callers can request a
// vector matching the DB column width (config.EmbeddingDim), even if that's
// larger than the natural Dim — extra dims are simply zero.
func ItemVector(item models.Item, targetDim int) []float32 {
	v := make([]float32, targetDim)

	// Genre block (multi-hot).
	for _, g := range item.Genres {
		if idx, ok := genreIndex[g]; ok && idx < targetDim {
			v[idx] = 1.0
		}
	}

	// Recency feature: newer items score higher. Clamp year to [1970, 2030].
	if base := len(Genres); base+0 < targetDim {
		year := float64(item.Year)
		if year < 1970 {
			year = 1970
		}
		v[base+0] = float32((year - 1970) / (2030 - 1970))
	}
	// Rating feature: normalize 0..5 -> 0..1.
	if base := len(Genres); base+1 < targetDim {
		v[base+1] = float32(item.Rating / 5.0)
	}

	Normalize(v)
	return v
}

// Normalize scales v in place to unit L2 length. A zero vector is left as-is.
func Normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

// Cosine returns the cosine similarity of a and b in [-1, 1]. If either is not
// unit-length this still returns the true cosine (we divide by the norms). For
// already-normalized vectors it is just the dot product. Mismatched lengths
// return 0.
func Cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// Average returns the element-wise mean of the given vectors, L2-normalized.
// This is how a user embedding is formed from the items they interacted with
// (see UserProfile.Embedding). Vectors of differing lengths are skipped defensively.
func Average(vectors [][]float32, dim int) []float32 {
	out := make([]float32, dim)
	var n float32
	for _, vec := range vectors {
		if len(vec) != dim {
			continue
		}
		for i := range vec {
			out[i] += vec[i]
		}
		n++
	}
	if n > 0 {
		for i := range out {
			out[i] /= n
		}
	}
	Normalize(out)
	return out
}
