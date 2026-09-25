## Project H: Real-Time Recommendation Engine (Go + Kafka + Redis)

### Why This Project

"Design a recommendation system" is the most common ML system design interview question. It appears at every FAANG company, every ML-focused startup, and most senior backend interviews. But almost nobody has actually built one end-to-end. They've used collaborative filtering in a Jupyter notebook — they haven't built the real-time serving, feature ingestion, candidate generation, and ranking pipeline that a production recommendation system requires.

You have a unique advantage: you understand embeddings (from RAG work), you understand real-time data pipelines (from your Kafka learning), and you can build the serving layer in Go. Most candidates can only talk about recommendations theoretically. You can point to a working system.

This project is also the natural companion to Project F (Feature Store). The feature store feeds the recommendation engine with real-time user features. If you build both, they form a coherent ML infrastructure stack.

### Architecture

```
  User Activity Events                                                         
  (views, clicks, purchases, ratings)                                         
       │                                                                       
       │  Kafka topic: user-events                                             
       ▼                                                                       
┌──────────────────────────────────────────────────────────────────────────────┐
│  Recommendation Engine                                                       │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Event Processor (Go, consumes from Kafka)                           │   │
│  │                                                                      │   │
│  │  On each event:                                                      │   │
│  │  1. Update user profile in Redis                                     │   │
│  │     (recent items viewed, categories, avg price range)               │   │
│  │  2. Update item popularity counters                                  │   │
│  │     (trending score with time decay)                                 │   │
│  │  3. Update co-occurrence matrix                                      │   │
│  │     (users who viewed X also viewed Y)                               │   │
│  │  4. Trigger real-time embedding update                               │   │
│  │     (if user profile shifted significantly)                          │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Recommendation API (Go + Gin)                                       │   │
│  │                                                                      │   │
│  │  GET /api/v1/recommend/:user_id?n=20&context=homepage                │   │
│  │                                                                      │   │
│  │  Pipeline (executed in ~50ms):                                       │   │
│  │                                                                      │   │
│  │  ┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐  │   │
│  │  │ Candidate  │──▶│ Candidate  │──▶│  Ranking   │──▶│  Post-     │  │   │
│  │  │ Retrieval  │   │ Filtering  │   │  Model     │   │  Processing│  │   │
│  │  │            │   │            │   │            │   │            │  │   │
│  │  │ - ANN      │   │ - Already  │   │ - Score    │   │ - Dedup    │  │   │
│  │  │   search   │   │   seen     │   │   each     │   │ - Diversity│  │   │
│  │  │   (top 200)│   │ - Out of   │   │   candidate│   │   boost    │  │   │
│  │  │ - Trending │   │   stock    │   │   using    │   │ - Business │  │   │
│  │  │ - "Also    │   │ - Age      │   │   features │   │   rules    │  │   │
│  │  │   viewed"  │   │   restrict │   │            │   │ - Return   │  │   │
│  │  │            │   │            │   │            │   │   top 20   │  │   │
│  │  └────────────┘   └────────────┘   └────────────┘   └────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Data Stores                                                         │   │
│  │                                                                      │   │
│  │  Redis:                                                              │   │
│  │  - User profiles (recent activity, preferences)                      │   │
│  │  - Item metadata cache                                               │   │
│  │  - Co-occurrence counts                                              │   │
│  │  - Trending scores (sorted set with time-decayed scores)             │   │
│  │                                                                      │   │
│  │  Vector Store (pgvector or Project E):                               │   │
│  │  - Item embeddings (content-based: embed item descriptions)          │   │
│  │  - User embeddings (collaborative: learned from interaction matrix)  │   │
│  │  - ANN search for candidate retrieval                                │   │
│  │                                                                      │   │
│  │  PostgreSQL:                                                         │   │
│  │  - Item catalog (id, title, category, price, metadata)               │   │
│  │  - Interaction log (user_id, item_id, event_type, timestamp)         │   │
│  │  - Recommendation log (what was shown, what was clicked — for eval)  │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Batch Pipeline (Airflow, nightly)                                   │   │
│  │                                                                      │   │
│  │  1. Retrain user/item embeddings from full interaction history        │   │
│  │  2. Rebuild co-occurrence matrix                                     │   │
│  │  3. Update popularity scores                                         │   │
│  │  4. Evaluate recommendation quality:                                  │  │
│  │     - Offline: precision@K, recall@K, nDCG on held-out interactions  │   │
│  │     - Online: click-through rate on yesterday's recommendations      │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Prometheus: recommendation_latency, candidate_pool_size,                    │
│              cache_hit_rate, click_through_rate                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### The Two-Stage Architecture (Retrieval + Ranking)

This is how Netflix, YouTube, and Amazon recommendations work. You can't score every item in the catalog for every request — too slow. Instead:

**Stage 1 — Candidate Retrieval (fast, imprecise, wide net):**

Pull ~200 candidates from multiple sources in parallel:

```go
func (s *RecommendationService) RetrieveCandidates(ctx context.Context, userID string, n int) []Candidate {
    user := s.redis.GetUserProfile(ctx, userID)
    
    var mu sync.Mutex
    var candidates []Candidate
    
    var wg sync.WaitGroup
    
    // Source 1: ANN search — items similar to user's embedding
    wg.Add(1)
    go func() {
        defer wg.Done()
        results := s.vectorStore.Search(user.Embedding, 100)
        mu.Lock()
        candidates = append(candidates, toCandidates(results, "embedding")...)
        mu.Unlock()
    }()
    
    // Source 2: "Also viewed" — collaborative filtering
    wg.Add(1)
    go func() {
        defer wg.Done()
        recentItems := user.RecentlyViewed[:min(10, len(user.RecentlyViewed))]
        for _, itemID := range recentItems {
            similar := s.redis.GetCoOccurrence(ctx, itemID, 10)
            mu.Lock()
            candidates = append(candidates, toCandidates(similar, "collab")...)
            mu.Unlock()
        }
    }()
    
    // Source 3: Trending items (popularity with recency decay)
    wg.Add(1)
    go func() {
        defer wg.Done()
        trending := s.redis.GetTrending(ctx, user.PreferredCategories, 50)
        mu.Lock()
        candidates = append(candidates, toCandidates(trending, "trending")...)
        mu.Unlock()
    }()
    
    // Source 4: New items in preferred categories (exploration)
    wg.Add(1)
    go func() {
        defer wg.Done()
        newItems := s.db.GetRecentItems(ctx, user.PreferredCategories, 30, 7) // last 7 days
        mu.Lock()
        candidates = append(candidates, toCandidates(newItems, "exploration")...)
        mu.Unlock()
    }()
    
    wg.Wait()
    
    // Deduplicate
    return deduplicate(candidates)
}
```

**Stage 2 — Ranking (slower, precise, narrow focus):**

Score each of the ~200 candidates using a feature-rich model. In production, this is an ML model (XGBoost, a small neural net). For this project, start with a weighted scoring formula, then optionally upgrade to a trained model:

```go
type Ranker struct {
    weights RankingWeights
}

type RankingWeights struct {
    EmbeddingSimilarity float64  // how similar to user's taste
    Popularity          float64  // trending score
    Recency             float64  // how new the item is
    PriceMatch          float64  // how close to user's price range
    CategoryMatch       float64  // is it in a preferred category
    SourceBoost         map[string]float64  // boost by retrieval source
}

func (r *Ranker) Score(user *UserProfile, item *ItemCandidate) float64 {
    score := 0.0
    
    // Embedding similarity (from ANN search)
    score += r.weights.EmbeddingSimilarity * item.SimilarityScore
    
    // Popularity (log-scaled to prevent popular items from dominating)
    score += r.weights.Popularity * math.Log1p(item.PopularityScore)
    
    // Recency (exponential decay: newer = higher)
    daysSinceCreated := time.Since(item.CreatedAt).Hours() / 24
    score += r.weights.Recency * math.Exp(-0.1*daysSinceCreated)
    
    // Price match (gaussian: penalize items far from user's avg spend)
    priceDiff := math.Abs(item.Price - user.AvgPurchasePrice)
    priceStd := user.PurchasePriceStd
    if priceStd > 0 {
        score += r.weights.PriceMatch * math.Exp(-0.5*math.Pow(priceDiff/priceStd, 2))
    }
    
    // Category match
    if contains(user.PreferredCategories, item.Category) {
        score += r.weights.CategoryMatch
    }
    
    // Source boost
    if boost, ok := r.weights.SourceBoost[item.Source]; ok {
        score *= boost
    }
    
    return score
}
```

**Post-Processing — Diversity:**

Without diversity injection, recommendations converge on a narrow set of popular items. The MMR (Maximal Marginal Relevance) algorithm balances relevance with diversity:

```go
func diversify(ranked []ScoredItem, topN int, lambda float64) []ScoredItem {
    selected := []ScoredItem{ranked[0]}  // always include the top-ranked item
    remaining := ranked[1:]
    
    for len(selected) < topN && len(remaining) > 0 {
        bestIdx := 0
        bestScore := math.Inf(-1)
        
        for i, candidate := range remaining {
            // Relevance: the original ranking score
            relevance := candidate.Score
            
            // Redundancy: max similarity to any already-selected item
            maxSim := 0.0
            for _, sel := range selected {
                sim := cosineSimilarity(candidate.Embedding, sel.Embedding)
                if sim > maxSim {
                    maxSim = sim
                }
            }
            
            // MMR: balance relevance vs novelty
            mmrScore := lambda*relevance - (1-lambda)*maxSim
            
            if mmrScore > bestScore {
                bestScore = mmrScore
                bestIdx = i
            }
        }
        
        selected = append(selected, remaining[bestIdx])
        remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
    }
    
    return selected
}
```

### Evaluation (Offline + Online)

**Offline (on held-out interaction data):**
- Precision@K: of the top-K recommended items, how many did the user actually interact with?
- Recall@K: of all items the user interacted with, how many were in the top-K?
- nDCG: measures ranking quality — did the items the user liked appear at the top?
- Coverage: what percentage of the catalog appears in recommendations? (low coverage = filter bubble)

**Online (A/B test on live traffic):**
- Click-through rate (CTR): % of recommended items that were clicked.
- Conversion rate: % of recommended items that were purchased.
- Diversity score: average pairwise distance between recommended items.

Log every recommendation served and every resulting interaction. This feedback loop is how the system improves over time.

### Benchmarks

- **Serving latency:** end-to-end from request to response. Target: under 50ms p99 (this is real-time, users are waiting).
- **Throughput:** requests per second. Target: 5,000+ on a single Go instance.
- **Offline metrics:** run on a public dataset like MovieLens-25M or Amazon Product Reviews.
- **A/B test simulation:** compare "popularity only" vs "embedding-based" vs "full pipeline" on the same dataset.

---
---