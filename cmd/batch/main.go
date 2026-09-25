// Command batch is the "nightly" cold-path job. It stands in for Airflow (see
// docs/01 D9 and docs/10): a single linear job you run on a cron. It performs the
// maintenance the online path can't do well itself:
//
//  1. Recompute item embeddings from the catalog and rewrite them (picks up any
//     change to the embedding function — the seam where a real training job goes).
//  2. Rebuild the co-occurrence matrix EXACTLY from the full interaction history,
//     correcting any online drift from at-least-once double-counting (docs/06).
//  3. Rescale trending scores so the exponential-increment decay can't overflow
//     (docs/05) — dividing all scores by a constant preserves ranking.
//
// Offline quality evaluation (precision@k, nDCG, coverage) is the fourth nightly
// task; it lives in its own command (`make eval`) so it can also be run ad hoc.
//
//	go run ./cmd/batch
package main

import (
	"context"
	"log"
	"time"

	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/dataset"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
)

// coWindow is how many recent items an event co-occurs with — must match the
// online processor's window so the rebuild reproduces the same structure.
const coWindow = 20

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pg, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()
	rs, err := redisstore.New(ctx, cfg.RedisAddr, cfg.TrendingHalfLife)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rs.Close()

	// 1. Recompute + rewrite item embeddings.
	log.Println("[1/3] recomputing item embeddings...")
	items, _, err := pg.AllItems(ctx)
	if err != nil {
		log.Fatalf("load items: %v", err)
	}
	embs := dataset.Embed(items, cfg.EmbeddingDim)
	if err := pg.SetEmbeddings(ctx, embs); err != nil {
		log.Fatalf("set embeddings: %v", err)
	}
	log.Printf("      rewrote %d embeddings", len(embs))

	// 2. Rebuild co-occurrence from full history.
	log.Println("[2/3] rebuilding co-occurrence from history...")
	entries, err := rebuildCoOccurrence(ctx, pg)
	if err != nil {
		log.Fatalf("rebuild co-occurrence: %v", err)
	}
	deleted, err := rs.ResetCoOccurrence(ctx)
	if err != nil {
		log.Fatalf("reset co-occurrence: %v", err)
	}
	if err := rs.AddCoOccurrenceBatch(ctx, entries); err != nil {
		log.Fatalf("write co-occurrence: %v", err)
	}
	log.Printf("      cleared %d old sets, wrote %d co-occurrence pairs", deleted, len(entries))

	// 3. Rescale trending to prevent unbounded growth.
	log.Println("[3/3] rescaling trending scores...")
	factor, err := rs.RescaleTrending(ctx)
	if err != nil {
		log.Fatalf("rescale trending: %v", err)
	}
	if factor > 0 {
		log.Printf("      divided all trending scores by %.3g", factor)
	} else {
		log.Printf("      no rescale needed")
	}

	log.Println("batch complete. Run `make eval` for offline quality metrics.")
}

// rebuildCoOccurrence recomputes symmetric co-occurrence weights from the full
// interaction log, mirroring the online rule: each event co-occurs with the
// items already in the user's recent window (weighted by event strength).
func rebuildCoOccurrence(ctx context.Context, pg *postgres.Store) ([]redisstore.CoOccEntry, error) {
	inter, err := pg.AllInteractions(ctx)
	if err != nil {
		return nil, err
	}
	// pairKey packs an unordered (a,b) pair with a<b into one int64 map key via a
	// nested map to avoid collisions.
	weights := map[int64]map[int64]float64{}
	recent := map[int64][]int64{} // per-user recent window (most-recent-first)

	add := func(a, b int64, w float64) {
		if a == b {
			return
		}
		if a > b {
			a, b = b, a
		}
		if weights[a] == nil {
			weights[a] = map[int64]float64{}
		}
		weights[a][b] += w
	}

	for _, e := range inter {
		w := e.Type.Weight()
		for _, prev := range recent[e.UserID] {
			add(e.ItemID, prev, w)
		}
		// push to recent window
		win := append([]int64{e.ItemID}, recent[e.UserID]...)
		if len(win) > coWindow {
			win = win[:coWindow]
		}
		recent[e.UserID] = win
	}

	var out []redisstore.CoOccEntry
	for a, inner := range weights {
		for b, w := range inner {
			out = append(out, redisstore.CoOccEntry{A: a, B: b, Weight: w})
		}
	}
	return out, nil
}
