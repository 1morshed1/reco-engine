package recommender

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/1morshed1/reco-engine/internal/ranking"
)

// fakeProfiles is an in-memory ProfileStore.
type fakeProfiles struct {
	profile  models.UserProfile
	err      error
	trending map[int64]float64
}

func (f *fakeProfiles) GetProfile(_ context.Context, userID int64, _, _ int) (models.UserProfile, error) {
	p := f.profile
	p.UserID = userID
	return p, f.err
}

func (f *fakeProfiles) TrendingScores(_ context.Context, ids []int64) (map[int64]float64, error) {
	out := make(map[int64]float64, len(ids))
	for _, id := range ids {
		out[id] = f.trending[id]
	}
	return out, nil
}

// fakeRetriever returns a fixed candidate pool.
type fakeRetriever struct{ pool []models.Candidate }

func (f *fakeRetriever) Retrieve(context.Context, models.UserProfile) []models.Candidate {
	out := make([]models.Candidate, len(f.pool))
	copy(out, f.pool)
	return out
}

// capturingRanker records what it was asked to rank, then delegates to the
// real ranker so ordering behaves as in production.
type capturingRanker struct {
	got  []models.Candidate
	real *ranking.Ranker
}

func (c *capturingRanker) Rank(p models.UserProfile, cands []models.Candidate) []models.ScoredItem {
	c.got = append([]models.Candidate(nil), cands...)
	return c.real.Rank(p, cands)
}

// fakeLogger records logged lists; logging is async so it signals via done.
type fakeLogger struct {
	mu    sync.Mutex
	items []models.ScoredItem
	done  chan struct{}
}

func (f *fakeLogger) LogRecommendations(_ context.Context, _ int64, items []models.ScoredItem) error {
	f.mu.Lock()
	f.items = items
	f.mu.Unlock()
	close(f.done)
	return nil
}

func cand(id int64, cat string, sim float64, emb []float32) models.Candidate {
	return models.Candidate{
		Item:            models.Item{ID: id, Category: cat, Rating: 3, CreatedAt: time.Now()},
		Source:          "embedding",
		SimilarityScore: sim,
		Embedding:       emb,
	}
}

func newService(p *fakeProfiles, pool []models.Candidate, cfg Config) (*Service, *capturingRanker) {
	rk := &capturingRanker{real: ranking.New(ranking.DefaultWeights())}
	return New(p, &fakeLogger{done: make(chan struct{})}, &fakeRetriever{pool: pool}, rk, cfg), rk
}

func noLogConfig() Config {
	cfg := DefaultConfig()
	cfg.LogRecs = false
	return cfg
}

// Items the user recently viewed must never be recommended back to them.
func TestRecommendFiltersSeenItems(t *testing.T) {
	profiles := &fakeProfiles{profile: models.UserProfile{RecentlyViewed: []int64{1, 3}}}
	pool := []models.Candidate{
		cand(1, "SciFi", 0.9, []float32{1, 0}),
		cand(2, "SciFi", 0.8, []float32{0, 1}),
		cand(3, "Drama", 0.7, []float32{1, 1}),
		cand(4, "Drama", 0.6, []float32{1, -1}),
	}
	svc, _ := newService(profiles, pool, noLogConfig())

	res, err := svc.Recommend(context.Background(), 42, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.UserID != 42 {
		t.Fatalf("user id: got %d", res.UserID)
	}
	if res.CandidatePoolSize != 4 {
		t.Fatalf("pool size should be reported before filtering: got %d", res.CandidatePoolSize)
	}
	for _, it := range res.Items {
		if it.Item.ID == 1 || it.Item.ID == 3 {
			t.Fatalf("seen item %d was recommended", it.Item.ID)
		}
	}
	if len(res.Items) != 2 {
		t.Fatalf("expected 2 unseen items, got %d", len(res.Items))
	}
}

// The response is capped at n even when the pool is larger.
func TestRecommendRespectsN(t *testing.T) {
	var pool []models.Candidate
	for i := int64(1); i <= 50; i++ {
		pool = append(pool, cand(i, "SciFi", float64(i)/50, []float32{float32(i), 1}))
	}
	svc, _ := newService(&fakeProfiles{}, pool, noLogConfig())

	res, err := svc.Recommend(context.Background(), 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(res.Items))
	}
}

// Raw trending scores are normalized to [0,1] within the pool before ranking.
func TestRecommendNormalizesPopularity(t *testing.T) {
	profiles := &fakeProfiles{trending: map[int64]float64{1: 1e9, 2: 5e8, 3: 0}}
	pool := []models.Candidate{
		cand(1, "A", 0.1, []float32{1, 0}),
		cand(2, "B", 0.1, []float32{0, 1}),
		cand(3, "C", 0.1, []float32{1, 1}),
	}
	svc, rk := newService(profiles, pool, noLogConfig())

	if _, err := svc.Recommend(context.Background(), 1, 3); err != nil {
		t.Fatal(err)
	}
	want := map[int64]float64{1: 1.0, 2: 0.5, 3: 0}
	for _, c := range rk.got {
		if c.PopularityScore != want[c.Item.ID] {
			t.Fatalf("item %d popularity: got %v want %v", c.Item.ID, c.PopularityScore, want[c.Item.ID])
		}
	}
}

// A cold-start user (no history) still gets recommendations from the pool.
func TestRecommendColdStart(t *testing.T) {
	pool := []models.Candidate{
		{Item: models.Item{ID: 7, Category: "Comedy", Rating: 4, CreatedAt: time.Now()}, Source: "trending"},
		{Item: models.Item{ID: 8, Category: "Drama", Rating: 3, CreatedAt: time.Now()}, Source: "fresh"},
	}
	svc, _ := newService(&fakeProfiles{}, pool, noLogConfig())

	res, err := svc.Recommend(context.Background(), 999, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("cold-start user should get the full pool, got %d", len(res.Items))
	}
}

// A profile-store failure surfaces as an error, not an empty result.
func TestRecommendProfileError(t *testing.T) {
	svc, _ := newService(&fakeProfiles{err: errors.New("redis down")}, nil, noLogConfig())
	if _, err := svc.Recommend(context.Background(), 1, 10); err == nil {
		t.Fatal("expected error when profile store fails")
	}
}

// An empty pool yields an empty, non-error result.
func TestRecommendEmptyPool(t *testing.T) {
	svc, _ := newService(&fakeProfiles{}, nil, noLogConfig())
	res, err := svc.Recommend(context.Background(), 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 0 {
		t.Fatalf("expected no items, got %d", len(res.Items))
	}
}

// Served lists are logged asynchronously when logging is enabled.
func TestRecommendLogsServedList(t *testing.T) {
	logger := &fakeLogger{done: make(chan struct{})}
	pool := []models.Candidate{cand(1, "A", 0.5, []float32{1, 0}), cand(2, "B", 0.4, []float32{0, 1})}
	rk := &capturingRanker{real: ranking.New(ranking.DefaultWeights())}
	svc := New(&fakeProfiles{}, logger, &fakeRetriever{pool: pool}, rk, DefaultConfig())

	res, err := svc.Recommend(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-logger.done:
	case <-time.After(2 * time.Second):
		t.Fatal("served list was not logged")
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.items) != len(res.Items) {
		t.Fatalf("logged %d items, served %d", len(logger.items), len(res.Items))
	}
}
