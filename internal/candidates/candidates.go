// Package candidates implements Stage 1 of the pipeline: retrieval. It pulls a
// wide, imprecise pool of ~200 plausible items from FOUR sources in parallel,
// then deduplicates them (see docs/07). Speed and recall matter here; precision
// is Stage 2's job (ranking).
//
// The four sources cover each other's blind spots:
//   - embedding : content match to the user's taste vector (ANN)      -> filter bubble alone
//   - collab    : "users who viewed X also viewed Y" (co-occurrence)   -> cold-start blind
//   - trending  : popular-right-now in the user's categories          -> not personalized
//   - fresh     : new items in preferred categories (exploration)     -> quality unknown
// Blended, they produce a robust pool for any user, including brand-new ones.
package candidates

import (
	"context"
	"sync"

	"github.com/1morshed1/reco-engine/internal/catalog"
	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
)

// Config sizes each retrieval source.
type Config struct {
	ANNTopK       int // items from embedding ANN search
	RecentWindow  int // how many recent items to seed co-occurrence from
	CoOccPerItem  int // co-occurring items to pull per recent item
	TrendingK     int // trending items to pull
	FreshK        int // fresh items to pull
	FreshDays     int // "fresh" = created within this many days
}

// DefaultConfig returns sizes that yield a ~200-candidate pool.
func DefaultConfig() Config {
	return Config{ANNTopK: 100, RecentWindow: 10, CoOccPerItem: 10, TrendingK: 50, FreshK: 30, FreshDays: 30}
}

// Retriever fans out to the stores. Postgres is used only for ANN (it needs the
// pgvector index); item hydration and fresh listing are served from the in-memory
// catalog to keep the hot path off the database (see docs/11).
type Retriever struct {
	redis *redisstore.Store
	pg    *postgres.Store
	cat   *catalog.Store
	cfg   Config
}

// New builds a Retriever.
func New(rs *redisstore.Store, pg *postgres.Store, cat *catalog.Store, cfg Config) *Retriever {
	return &Retriever{redis: rs, pg: pg, cat: cat, cfg: cfg}
}

// Retrieve runs all four sources concurrently and returns the deduplicated pool.
// Errors from any single source are tolerated (logged by the caller via the
// returned error slice) — a degraded source shouldn't fail the whole request.
func (r *Retriever) Retrieve(ctx context.Context, p models.UserProfile) []models.Candidate {
	var mu sync.Mutex
	var all []models.Candidate
	add := func(c []models.Candidate) {
		mu.Lock()
		all = append(all, c...)
		mu.Unlock()
	}

	var wg sync.WaitGroup

	// Source 1: embedding ANN (only if the user has a taste vector yet).
	if len(p.Embedding) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c, err := r.pg.ANNSearch(ctx, p.Embedding, r.cfg.ANNTopK); err == nil {
				add(c)
			}
		}()
	}

	// Source 2: co-occurrence ("also viewed") from the user's recent items.
	wg.Add(1)
	go func() {
		defer wg.Done()
		seed := p.RecentlyViewed
		if len(seed) > r.cfg.RecentWindow {
			seed = seed[:r.cfg.RecentWindow]
		}
		idSet := map[int64]struct{}{}
		for _, itemID := range seed {
			ids, err := r.redis.CoOccurring(ctx, itemID, r.cfg.CoOccPerItem)
			if err != nil {
				continue
			}
			for _, id := range ids {
				idSet[id] = struct{}{}
			}
		}
		add(r.cat.ByIDs(keys(idSet), "collab"))
	}()

	// Source 3: trending in the user's preferred categories.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := r.redis.Trending(ctx, p.PreferredCategories, r.cfg.TrendingK)
		if err != nil {
			return
		}
		add(r.cat.ByIDs(ids, "trending"))
	}()

	// Source 4: fresh items (exploration) — served from the in-memory catalog.
	wg.Add(1)
	go func() {
		defer wg.Done()
		add(r.cat.Fresh(p.PreferredCategories, r.cfg.FreshK, r.cfg.FreshDays))
	}()

	wg.Wait()
	return deduplicate(all)
}

// deduplicate collapses items appearing from multiple sources into one candidate.
// It keeps the first occurrence but preserves the best signals: a non-zero
// embedding similarity from the ANN source is carried over even if that item was
// first seen via trending/collab (so ranking still benefits from it).
func deduplicate(in []models.Candidate) []models.Candidate {
	idx := make(map[int64]int, len(in))
	out := make([]models.Candidate, 0, len(in))
	for _, c := range in {
		if i, ok := idx[c.Item.ID]; ok {
			if c.SimilarityScore > out[i].SimilarityScore {
				out[i].SimilarityScore = c.SimilarityScore
			}
			if len(out[i].Embedding) == 0 && len(c.Embedding) > 0 {
				out[i].Embedding = c.Embedding
			}
			continue
		}
		idx[c.Item.ID] = len(out)
		out = append(out, c)
	}
	return out
}

func keys(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
