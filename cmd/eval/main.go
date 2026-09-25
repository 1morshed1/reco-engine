// Command eval measures recommendation quality OFFLINE, comparing three
// strategies on a held-out test set (see docs/09):
//
//	popularity : recommend the globally most-interacted items (no personalization)
//	embedding  : nearest items to the user's taste vector (content/ANN)
//	full       : a blended score (similarity + popularity + category + rating)
//
// Methodology — leakage-free by construction:
//   - Per user, interactions are split TEMPORALLY: the earliest 80% is "train"
//     (history the recommender may use), the latest 20% is "test" (the items it
//     must predict). A recommender that peeked at the test items would be
//     cheating, so every signal here is rebuilt from TRAIN ONLY — we do NOT read
//     the live Redis state (which was built from all events).
//   - Recommendations exclude items already seen in train (you don't recommend
//     what someone already engaged with).
//
// Metrics: precision@K, recall@K, nDCG@K (ranking quality), and catalog coverage
// (what fraction of the catalog ever gets recommended — low = filter bubble).
//
//	go run ./cmd/eval           # K=10
//	go run ./cmd/eval -k 20
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/embedding"
	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
)

func main() {
	k := flag.Int("k", 10, "cutoff K for precision/recall/nDCG")
	minInter := flag.Int("min", 5, "minimum interactions for a user to be evaluated")
	flag.Parse()

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer db.Close()

	items, embs, err := db.AllItems(ctx)
	if err != nil {
		log.Fatalf("load items: %v", err)
	}
	itemByID := make(map[int64]models.Item, len(items))
	for _, it := range items {
		itemByID[it.ID] = it
	}

	inter, err := db.AllInteractions(ctx)
	if err != nil {
		log.Fatalf("load interactions: %v", err)
	}
	log.Printf("loaded %d items, %d interactions", len(items), len(inter))

	// Group interactions by user (already chronological from SQL).
	byUser := map[int64][]models.Event{}
	for _, e := range inter {
		byUser[e.UserID] = append(byUser[e.UserID], e)
	}

	// Global popularity from TRAIN portions only (filled during the per-user loop
	// via a first pass over train items). We accumulate here.
	globalPop := map[int64]float64{}

	// Per-user train/test, computed once and reused across strategies.
	type split struct {
		train map[int64]struct{}
		test  map[int64]struct{}
		emb   []float32
		cats  map[string]float64
	}
	splits := map[int64]split{}

	for uid, evs := range byUser {
		if len(evs) < *minInter {
			continue
		}
		cut := int(float64(len(evs)) * 0.8)
		if cut < 1 {
			cut = 1
		}
		if cut >= len(evs) {
			cut = len(evs) - 1
		}
		trainEvs, testEvs := evs[:cut], evs[cut:]

		train := map[int64]struct{}{}
		var trainVecs [][]float32
		cats := map[string]float64{}
		for _, e := range trainEvs {
			train[e.ItemID] = struct{}{}
			globalPop[e.ItemID] += e.Type.Weight()
			if v, ok := embs[e.ItemID]; ok {
				trainVecs = append(trainVecs, v)
			}
			if it, ok := itemByID[e.ItemID]; ok {
				cats[it.Category] += e.Type.Weight()
			}
		}
		test := map[int64]struct{}{}
		for _, e := range testEvs {
			if _, seen := train[e.ItemID]; !seen {
				test[e.ItemID] = struct{}{}
			}
		}
		if len(test) == 0 {
			continue
		}
		splits[uid] = split{
			train: train,
			test:  test,
			emb:   embedding.Average(trainVecs, cfg.EmbeddingDim),
			cats:  cats,
		}
	}
	log.Printf("evaluating %d users at K=%d", len(splits), *k)

	// Precompute a popularity-ranked item list once (shared by all users).
	popRanked := rankByScore(globalPop)
	maxPop := 0.0
	for _, v := range globalPop {
		if v > maxPop {
			maxPop = v
		}
	}

	strategies := []string{"popularity", "embedding", "full"}
	agg := map[string]*metrics{}
	covered := map[string]map[int64]struct{}{}
	for _, s := range strategies {
		agg[s] = &metrics{}
		covered[s] = map[int64]struct{}{}
	}

	for _, sp := range splits {
		for _, strat := range strategies {
			recs := recommend(strat, *k, sp.train, sp.emb, sp.cats, items, embs, itemByID, popRanked, globalPop, maxPop)
			for _, id := range recs {
				covered[strat][id] = struct{}{}
			}
			p, r, n := score(recs, sp.test, *k)
			m := agg[strat]
			m.precision += p
			m.recall += r
			m.ndcg += n
			m.users++
		}
	}

	// Report.
	fmt.Printf("\n=== Offline evaluation (K=%d, users=%d, catalog=%d) ===\n", *k, len(splits), len(items))
	fmt.Printf("%-12s  %-10s  %-10s  %-10s  %-10s\n", "strategy", "precision", "recall", "nDCG", "coverage")
	fmt.Printf("%-12s  %-10s  %-10s  %-10s  %-10s\n", "--------", "---------", "------", "----", "--------")
	for _, s := range strategies {
		m := agg[s]
		u := float64(m.users)
		cov := float64(len(covered[s])) / float64(len(items))
		fmt.Printf("%-12s  %-10.4f  %-10.4f  %-10.4f  %-10.4f\n",
			s, m.precision/u, m.recall/u, m.ndcg/u, cov)
	}
	fmt.Println()
}

type metrics struct {
	precision, recall, ndcg float64
	users                   int
}

// recommend produces the top-K item IDs for a user under one strategy, excluding
// items already in train. All signals come from train-derived inputs only.
func recommend(strat string, k int, train map[int64]struct{}, userEmb []float32,
	cats map[string]float64, items []models.Item, embs map[int64][]float32,
	itemByID map[int64]models.Item, popRanked []int64, globalPop map[int64]float64, maxPop float64) []int64 {

	switch strat {
	case "popularity":
		out := make([]int64, 0, k)
		for _, id := range popRanked {
			if _, seen := train[id]; seen {
				continue
			}
			out = append(out, id)
			if len(out) == k {
				break
			}
		}
		return out

	case "embedding":
		type sc struct {
			id  int64
			sim float64
		}
		scored := make([]sc, 0, len(items))
		for _, it := range items {
			if _, seen := train[it.ID]; seen {
				continue
			}
			scored = append(scored, sc{it.ID, embedding.Cosine(userEmb, embs[it.ID])})
		}
		sort.Slice(scored, func(i, j int) bool { return scored[i].sim > scored[j].sim })
		return topIDs(scored, k, func(x sc) int64 { return x.id })

	default: // "full": blended score
		type sc struct {
			id    int64
			score float64
		}
		// derive preferred categories (top 3)
		pref := topCatNames(cats, 3)
		scored := make([]sc, 0, len(items))
		for _, it := range items {
			if _, seen := train[it.ID]; seen {
				continue
			}
			s := 1.0 * embedding.Cosine(userEmb, embs[it.ID])
			if maxPop > 0 {
				s += 0.3 * math.Log1p(globalPop[it.ID]/maxPop)
			}
			if containsStr(pref, it.Category) {
				s += 0.6
			}
			s += 0.3 * (it.Rating / 5.0)
			scored = append(scored, sc{it.ID, s})
		}
		sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
		return topIDs(scored, k, func(x sc) int64 { return x.id })
	}
}

// score computes precision@K, recall@K, nDCG@K for one user's rec list.
func score(recs []int64, test map[int64]struct{}, k int) (precision, recall, ndcg float64) {
	if len(recs) == 0 {
		return 0, 0, 0
	}
	hits := 0
	dcg := 0.0
	for i, id := range recs {
		if _, ok := test[id]; ok {
			hits++
			dcg += 1.0 / math.Log2(float64(i+2)) // rank i (0-based) -> position i+1
		}
	}
	precision = float64(hits) / float64(k)
	recall = float64(hits) / float64(len(test))
	// Ideal DCG: min(len(test), k) relevant items all at the top.
	ideal := len(test)
	if ideal > k {
		ideal = k
	}
	idcg := 0.0
	for i := 0; i < ideal; i++ {
		idcg += 1.0 / math.Log2(float64(i+2))
	}
	if idcg > 0 {
		ndcg = dcg / idcg
	}
	return precision, recall, ndcg
}

// --- small helpers ---

func rankByScore(m map[int64]float64) []int64 {
	type kv struct {
		id int64
		v  float64
	}
	arr := make([]kv, 0, len(m))
	for id, v := range m {
		arr = append(arr, kv{id, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
	out := make([]int64, len(arr))
	for i, x := range arr {
		out[i] = x.id
	}
	return out
}

func topIDs[T any](xs []T, k int, id func(T) int64) []int64 {
	if k > len(xs) {
		k = len(xs)
	}
	out := make([]int64, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, id(xs[i]))
	}
	return out
}

func topCatNames(counts map[string]float64, n int) []string {
	type kv struct {
		k string
		v float64
	}
	arr := make([]kv, 0, len(counts))
	for k, v := range counts {
		arr = append(arr, kv{k, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
	out := make([]string, 0, n)
	for i := 0; i < n && i < len(arr); i++ {
		out = append(out, arr[i].k)
	}
	return out
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
