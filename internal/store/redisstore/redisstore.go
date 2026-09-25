// Package redisstore holds the hot online state read on every recommendation
// request: user profiles, item co-occurrence, and trending scores (see docs/05).
//
// Everything here is DERIVED from the event stream and rebuildable — Redis is a
// cache of computed state, never a source of truth (docs/02). We lean on Redis's
// native data structures so reads are cheap:
//
//	recent:{u}     LIST    most-recent-first item IDs (LPUSH + LTRIM)
//	catcount:{u}   HASH    weighted interaction count per category (HINCRBYFLOAT)
//	price:{u}      HASH    running sum / sumsq / count for price mean+std
//	emb:{u}        STRING  JSON-encoded user embedding vector
//	meta:{u}       HASH    misc counters (interactions)
//	cooc:{item}    ZSET    co-viewed items, score = co-occurrence count
//	trending       ZSET    global trending, time-decayed score
//	trending:{cat} ZSET    per-category trending, time-decayed score
package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/redis/go-redis/v9"
)

// decayEpoch anchors the trending time-decay math. Increments are multiplied by
// exp(age/tau), so newer events contribute exponentially more — ranking by raw
// score then yields time-decayed popularity without ever rewriting old scores
// (the classic Hacker-News/Reddit trick). See docs/05 for the math and caveats.
var decayEpoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// Store wraps a go-redis client.
type Store struct {
	rdb *redis.Client
	tau float64 // decay time-constant in seconds (halfLife / ln2)
}

// New connects to Redis and pings it. halfLife controls trending decay.
func New(ctx context.Context, addr string, halfLife time.Duration) (*Store, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Store{rdb: rdb, tau: halfLife.Seconds() / math.Ln2}, nil
}

// Close releases the client.
func (s *Store) Close() error { return s.rdb.Close() }

// key helpers keep the naming scheme in one place.
func recentKey(u int64) string   { return "recent:" + itoa(u) }
func catKey(u int64) string      { return "catcount:" + itoa(u) }
func priceKey(u int64) string    { return "price:" + itoa(u) }
func embKey(u int64) string      { return "emb:" + itoa(u) }
func metaKey(u int64) string     { return "meta:" + itoa(u) }
func coocKey(item int64) string  { return "cooc:" + itoa(item) }
func trendCatKey(c string) string { return "trending:" + c }

const trendGlobalKey = "trending"

// --- profile writes (called by the processor) ---

// PushRecent prepends an item to the user's recently-viewed list and trims it to
// maxLen. Duplicates are allowed to age out naturally.
func (s *Store) PushRecent(ctx context.Context, userID, itemID int64, maxLen int) error {
	pipe := s.rdb.TxPipeline()
	pipe.LPush(ctx, recentKey(userID), itemID)
	pipe.LTrim(ctx, recentKey(userID), 0, int64(maxLen-1))
	_, err := pipe.Exec(ctx)
	return err
}

// IncrCategory adds weight to a category's counter for the user.
func (s *Store) IncrCategory(ctx context.Context, userID int64, category string, weight float64) error {
	return s.rdb.HIncrByFloat(ctx, catKey(userID), category, weight).Err()
}

// AddPrice folds a price into the user's running mean/std accumulators.
func (s *Store) AddPrice(ctx context.Context, userID int64, price float64) error {
	pipe := s.rdb.TxPipeline()
	pipe.HIncrByFloat(ctx, priceKey(userID), "sum", price)
	pipe.HIncrByFloat(ctx, priceKey(userID), "sumsq", price*price)
	pipe.HIncrByFloat(ctx, priceKey(userID), "count", 1)
	_, err := pipe.Exec(ctx)
	return err
}

// SetEmbedding stores the user's embedding vector (JSON-encoded).
func (s *Store) SetEmbedding(ctx context.Context, userID int64, vec []float32) error {
	b, err := json.Marshal(vec)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, embKey(userID), b, 0).Err()
}

// IncrInteractions bumps the user's lifetime interaction counter.
func (s *Store) IncrInteractions(ctx context.Context, userID int64) error {
	return s.rdb.HIncrBy(ctx, metaKey(userID), "interactions", 1).Err()
}

// GetEmbedding returns the user's stored embedding (nil if none yet).
func (s *Store) GetEmbedding(ctx context.Context, userID int64) ([]float32, error) {
	b, err := s.rdb.Get(ctx, embKey(userID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var v []float32
	return v, json.Unmarshal(b, &v)
}

// EmbWeight returns the accumulated weight behind the user's embedding average
// (0 if none). Used to fold a new item into the weighted running average.
func (s *Store) EmbWeight(ctx context.Context, userID int64) (float64, error) {
	v, err := s.rdb.HGet(ctx, metaKey(userID), "embweight").Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return v, err
}

// SetEmbWeight stores the accumulated embedding weight.
func (s *Store) SetEmbWeight(ctx context.Context, userID int64, w float64) error {
	return s.rdb.HSet(ctx, metaKey(userID), "embweight", w).Err()
}

// --- co-occurrence ---

// IncrCoOccurrence records that itemA and itemB were interacted with by the same
// user, in both directions (co-occurrence is symmetric).
func (s *Store) IncrCoOccurrence(ctx context.Context, itemA, itemB int64, delta float64) error {
	if itemA == itemB {
		return nil
	}
	pipe := s.rdb.TxPipeline()
	pipe.ZIncrBy(ctx, coocKey(itemA), delta, itoa(itemB))
	pipe.ZIncrBy(ctx, coocKey(itemB), delta, itoa(itemA))
	_, err := pipe.Exec(ctx)
	return err
}

// CoOccurring returns the top-k items most frequently co-interacted with itemID.
func (s *Store) CoOccurring(ctx context.Context, itemID int64, k int) ([]int64, error) {
	members, err := s.rdb.ZRevRange(ctx, coocKey(itemID), 0, int64(k-1)).Result()
	if err != nil {
		return nil, err
	}
	return toIDs(members), nil
}

// --- trending ---

// AddTrending adds a time-decayed weight to the global and per-category trending
// sorted sets for an item at time `now`.
func (s *Store) AddTrending(ctx context.Context, itemID int64, category string, weight float64, now time.Time) error {
	inc := weight * math.Exp(now.Sub(decayEpoch).Seconds()/s.tau)
	pipe := s.rdb.TxPipeline()
	pipe.ZIncrBy(ctx, trendGlobalKey, inc, itoa(itemID))
	if category != "" {
		pipe.ZIncrBy(ctx, trendCatKey(category), inc, itoa(itemID))
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Trending returns up to k trending item IDs. If categories are given it unions
// those category sets; otherwise it falls back to the global set. Deduplicated,
// preserving the highest-scoring appearance.
func (s *Store) Trending(ctx context.Context, categories []string, k int) ([]int64, error) {
	seen := map[int64]bool{}
	var out []int64
	pull := func(key string) error {
		members, err := s.rdb.ZRevRange(ctx, key, 0, int64(k-1)).Result()
		if err != nil {
			return err
		}
		for _, id := range toIDs(members) {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
		return nil
	}
	if len(categories) == 0 {
		if err := pull(trendGlobalKey); err != nil {
			return nil, err
		}
	} else {
		for _, c := range categories {
			if err := pull(trendCatKey(c)); err != nil {
				return nil, err
			}
		}
	}
	if len(out) > k {
		out = out[:k]
	}
	return out, nil
}

// TrendingScore returns the current raw trending score for an item (global set),
// used by the ranker as a popularity feature. Missing => 0.
func (s *Store) TrendingScore(ctx context.Context, itemID int64) (float64, error) {
	score, err := s.rdb.ZScore(ctx, trendGlobalKey, itoa(itemID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return score, err
}

// TrendingScores returns the current global trending score for each id (0 for
// missing), in one round-trip via ZMScore. Used to fill the popularity feature
// for ranking without N calls.
func (s *Store) TrendingScores(ctx context.Context, ids []int64) (map[int64]float64, error) {
	out := make(map[int64]float64, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	members := make([]string, len(ids))
	for i, id := range ids {
		members[i] = itoa(id)
	}
	scores, err := s.rdb.ZMScore(ctx, trendGlobalKey, members...).Result()
	if err != nil {
		return nil, err
	}
	for i, sc := range scores {
		out[ids[i]] = sc // missing members come back as 0
	}
	return out, nil
}

// RescaleTrending divides every trending score (global + per-category sets) by a
// common factor so the exponential-increment scores can't grow without bound
// (docs/05 overflow caveat). Dividing all scores by the same factor preserves
// ranking exactly. The factor is the current global maximum, which renames the
// top item's score to ~1. Returns the factor applied (0 if nothing to do).
//
// This is the production fix the batch layer performs periodically.
func (s *Store) RescaleTrending(ctx context.Context) (float64, error) {
	// Find the global max.
	top, err := s.rdb.ZRevRangeWithScores(ctx, trendGlobalKey, 0, 0).Result()
	if err != nil || len(top) == 0 {
		return 0, err
	}
	factor := top[0].Score
	if factor <= 1 {
		return 0, nil // already small; nothing to gain
	}
	// Collect every trending key (global + per-category).
	keys := []string{trendGlobalKey}
	iter := s.rdb.Scan(ctx, 0, "trending:*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}
	for _, key := range keys {
		members, err := s.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return 0, err
		}
		if len(members) == 0 {
			continue
		}
		z := make([]redis.Z, len(members))
		for i, m := range members {
			z[i] = redis.Z{Score: m.Score / factor, Member: m.Member}
		}
		if err := s.rdb.ZAdd(ctx, key, z...).Err(); err != nil {
			return 0, err
		}
	}
	return factor, nil
}

// ResetCoOccurrence deletes all co-occurrence sets so the batch layer can rebuild
// them exactly from history (correcting any online double-counting drift from
// at-least-once delivery; docs/06).
func (s *Store) ResetCoOccurrence(ctx context.Context) (int, error) {
	var keys []string
	iter := s.rdb.Scan(ctx, 0, "cooc:*", 200).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}
	for i := 0; i < len(keys); i += 500 {
		end := i + 500
		if end > len(keys) {
			end = len(keys)
		}
		if err := s.rdb.Del(ctx, keys[i:end]...).Err(); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

// CoOccEntry is one symmetric co-occurrence weight to add.
type CoOccEntry struct {
	A, B   int64
	Weight float64
}

// AddCoOccurrenceBatch writes many co-occurrence increments in one pipeline.
func (s *Store) AddCoOccurrenceBatch(ctx context.Context, entries []CoOccEntry) error {
	pipe := s.rdb.Pipeline()
	for _, e := range entries {
		if e.A == e.B {
			continue
		}
		pipe.ZIncrBy(ctx, coocKey(e.A), e.Weight, itoa(e.B))
		pipe.ZIncrBy(ctx, coocKey(e.B), e.Weight, itoa(e.A))
	}
	_, err := pipe.Exec(ctx)
	return err
}

// --- profile read (called by the API on every request) ---

// GetProfile assembles the user's live profile from the individual keys. Missing
// keys yield zero values (a brand-new user gets an empty-but-valid profile).
func (s *Store) GetProfile(ctx context.Context, userID int64, recentN, topCategories int) (models.UserProfile, error) {
	p := models.UserProfile{UserID: userID, CategoryCounts: map[string]float64{}}

	// Recently viewed.
	recent, err := s.rdb.LRange(ctx, recentKey(userID), 0, int64(recentN-1)).Result()
	if err != nil && err != redis.Nil {
		return p, err
	}
	p.RecentlyViewed = toIDs(recent)

	// Category counts -> preferred categories (top-N by weighted count).
	cats, err := s.rdb.HGetAll(ctx, catKey(userID)).Result()
	if err != nil && err != redis.Nil {
		return p, err
	}
	for c, v := range cats {
		f, _ := strconv.ParseFloat(v, 64)
		p.CategoryCounts[c] = f
	}
	p.PreferredCategories = topCats(p.CategoryCounts, topCategories)

	// Price mean/std from accumulators.
	price, err := s.rdb.HGetAll(ctx, priceKey(userID)).Result()
	if err != nil && err != redis.Nil {
		return p, err
	}
	sum, _ := strconv.ParseFloat(price["sum"], 64)
	sumsq, _ := strconv.ParseFloat(price["sumsq"], 64)
	count, _ := strconv.ParseFloat(price["count"], 64)
	if count > 0 {
		p.AvgPrice = sum / count
		variance := sumsq/count - p.AvgPrice*p.AvgPrice
		if variance > 0 {
			p.PriceStd = math.Sqrt(variance)
		}
	}

	// Embedding.
	if b, err := s.rdb.Get(ctx, embKey(userID)).Bytes(); err == nil {
		_ = json.Unmarshal(b, &p.Embedding)
	} else if err != redis.Nil {
		return p, err
	}

	// Interaction count.
	if v, err := s.rdb.HGet(ctx, metaKey(userID), "interactions").Int64(); err == nil {
		p.InteractionCount = v
	}

	return p, nil
}

// --- helpers ---

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func toIDs(members []string) []int64 {
	out := make([]int64, 0, len(members))
	for _, m := range members {
		if id, err := strconv.ParseInt(m, 10, 64); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// topCats returns the n category names with the highest counts, descending.
func topCats(counts map[string]float64, n int) []string {
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
