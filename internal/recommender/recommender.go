// Package recommender orchestrates the full request-time pipeline:
//
//	profile -> retrieve candidates -> filter -> fill features -> rank -> diversify
//
// It is the code path the latency budget applies to. Everything it touches is
// pre-computed state (Redis, pgvector), so the request does only light work.
package recommender

import (
	"context"
	"time"

	"github.com/1morshed1/reco-engine/internal/candidates"
	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/ranking"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
	"github.com/1morshed1/reco-engine/internal/diversity"
)

// Config tunes the orchestration.
type Config struct {
	RecentN         int     // recent items to load into the profile
	TopCategories   int     // preferred categories to derive
	DiversityLambda float64 // MMR relevance/novelty tradeoff
	RankPoolFactor  int     // MMR operates on top (n * factor) ranked items
	LogRecs         bool    // persist served lists for CTR/eval
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{RecentN: 20, TopCategories: 3, DiversityLambda: 0.7, RankPoolFactor: 3, LogRecs: true}
}

// Result is what a recommendation request returns.
type Result struct {
	UserID            int64               `json:"user_id"`
	Items             []models.ScoredItem `json:"items"`
	CandidatePoolSize int                 `json:"candidate_pool_size"`
	TookMS            float64             `json:"took_ms"`
}

// Service holds the collaborators.
type Service struct {
	redis     *redisstore.Store
	pg        *postgres.Store
	retriever *candidates.Retriever
	ranker    *ranking.Ranker
	cfg       Config
}

// New builds the service.
func New(rs *redisstore.Store, pg *postgres.Store, ret *candidates.Retriever, rk *ranking.Ranker, cfg Config) *Service {
	return &Service{redis: rs, pg: pg, retriever: ret, ranker: rk, cfg: cfg}
}

// Recommend runs the pipeline for a user and returns the top-n results.
func (s *Service) Recommend(ctx context.Context, userID int64, n int) (Result, error) {
	start := time.Now()

	// 1. Load the user's live profile (one Redis round-trip's worth of reads).
	profile, err := s.redis.GetProfile(ctx, userID, s.cfg.RecentN, s.cfg.TopCategories)
	if err != nil {
		return Result{}, err
	}

	// 2. Stage 1: retrieve the candidate pool (parallel, multi-source).
	pool := s.retriever.Retrieve(ctx, profile)
	poolSize := len(pool)

	// 3. Filter already-seen items (cheap: the recently-viewed set). A fuller
	//    "seen" filter would use a per-user bloom filter over all history; we
	//    keep it to the hot set to stay within the latency budget (docs/07).
	seen := make(map[int64]struct{}, len(profile.RecentlyViewed))
	for _, id := range profile.RecentlyViewed {
		seen[id] = struct{}{}
	}
	filtered := pool[:0:0]
	for _, c := range pool {
		if _, ok := seen[c.Item.ID]; !ok {
			filtered = append(filtered, c)
		}
	}

	// 4. Fill the popularity feature for every candidate in one ZMScore call.
	ids := make([]int64, len(filtered))
	for i, c := range filtered {
		ids[i] = c.Item.ID
	}
	if scores, err := s.redis.TrendingScores(ctx, ids); err == nil {
		for i := range filtered {
			filtered[i].PopularityScore = scores[filtered[i].Item.ID]
		}
	}

	// 5. Stage 2: rank all candidates by relevance.
	ranked := s.ranker.Rank(profile, filtered)

	// 6. Diversify the top pool with MMR into the final top-n. MMR is O(pool^2),
	//    so we only run it over the top (n * RankPoolFactor) ranked items.
	poolN := n * s.cfg.RankPoolFactor
	if poolN > len(ranked) {
		poolN = len(ranked)
	}
	final := diversity.MMR(ranked[:poolN], n, s.cfg.DiversityLambda)

	// 7. Log the served list for CTR / offline evaluation (best-effort, async).
	if s.cfg.LogRecs && len(final) > 0 {
		go func(items []models.ScoredItem) {
			lctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = s.pg.LogRecommendations(lctx, userID, items)
		}(final)
	}

	return Result{
		UserID:            userID,
		Items:             final,
		CandidatePoolSize: poolSize,
		TookMS:            float64(time.Since(start).Microseconds()) / 1000.0,
	}, nil
}
