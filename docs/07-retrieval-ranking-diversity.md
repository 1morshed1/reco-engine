# 07 — The Pipeline: Retrieval → Ranking → Diversity

This is the core of the whole project: how a request becomes a ranked, diverse
list. It covers `internal/candidates`, `internal/ranking`, `internal/diversity`,
and the `internal/recommender` orchestrator. Re-read docs/00's two-stage diagram
first — this doc is that diagram in detail.

## Why two stages (again, because it's the whole point)

You can't score a million-item catalog per request. So:
- **Stage 1 (retrieval)** is cheap and imprecise: cast a wide net, get ~200
  plausible items fast. Optimize for *recall* (don't miss good items) and speed.
- **Stage 2 (ranking)** is expensive and precise: score those ~200 carefully,
  get the *order* right. Optimize for *precision*.

Doing the expensive work on 200 items instead of 1,000,000 is what makes the
latency budget achievable.

## Stage 1 — Retrieval (`internal/candidates`)

Four sources run **concurrently** (goroutines + a mutex-guarded append), so total
time ≈ the slowest source, not the sum:

| Source     | Where from            | Signal                         | Blind spot it covers |
|------------|-----------------------|--------------------------------|----------------------|
| embedding  | pgvector ANN          | content match to taste vector  | (bubble if used alone)|
| collab     | Redis co-occurrence   | "also viewed" (collaborative)  | cold-start items     |
| trending   | Redis trending ZSET   | popular now, in your categories| personalization      |
| fresh      | Postgres recent items | new items to explore           | quality unknown      |

**Why blend?** Every single strategy has a failure mode (noted above). Content-
only recommends more of the same; collaborative-only can't handle new
users/items; popularity-only ignores the person. Blending covers each other's
gaps — and it's exactly how production systems build a candidate pool. A brand-new
user with no embedding still gets trending + fresh candidates (graceful cold
start).

**Dedup with signal preservation:** the same item can surface from several
sources. `deduplicate` keeps one copy but *carries over the best signals* — e.g.
if an item came from trending but also appeared in the ANN results, its embedding
similarity is preserved so ranking still benefits. First-seen source label wins.

## Stage 2 — Ranking (`internal/ranking`)

Each candidate gets one score from a **transparent weighted sum** over
interpretable features:

```
score =  w_sim      * embeddingSimilarity
       + w_pop      * log1p(popularity)          # log => diminishing returns
       + w_recency  * exp(-0.1 * daysOld)        # newer scores higher
       + w_price    * exp(-0.5 * z²)             # gaussian fit to price zone
       + w_category * [item.category ∈ preferred]
       + w_rating   * (rating / 5)
score *= sourceBoost[source]                      # trust some sources more
```

Design notes:
- **`log1p(popularity)`** stops a runaway-popular item from dominating purely on
  volume — popularity has diminishing returns.
- **Gaussian price match** uses the user's own price std: someone with erratic
  spending is penalized less for a mismatch. Skipped entirely for users with no
  price history.
- **Why a formula, not a model?** It's inspectable (you can read *why* an item
  ranked where), needs no training data, and — crucially — it consumes a
  **feature vector per candidate**, exactly like a real model. Swapping in a
  trained XGBoost/neural scorer later means replacing `Score()` and nothing else
  (docs/01 D7). Start simple, keep the seam.

## Post-processing — Diversity via MMR (`internal/diversity`)

Ranking alone converges on near-duplicates. **Maximal Marginal Relevance** fixes
that. It builds the result greedily: take the top item, then repeatedly pick the
item maximizing

```
mmr = λ * relevance − (1 − λ) * maxSimilarityToAlreadyChosen
```

- **λ → 1**: pure relevance (ranking order).
- **λ → 0**: pure novelty (maximally spread out).
- **λ ≈ 0.7** (our default): keep strong items while injecting variety.

Two important details:
- **Novelty = cosine similarity in embedding space** to already-selected items —
  we reuse the embeddings we already carried through retrieval. No extra data.
- **Scores are min-max normalized to [0,1] first.** Otherwise a raw relevance of
  3.0 would swamp the [0,1] similarity penalty and λ would have no effect. Putting
  both terms on the same scale is what makes λ meaningful.

**Cost:** MMR is O(pool²) in similarity comparisons, so we run it only over the
top `n * RankPoolFactor` ranked items (e.g. top 30 for n=10), not the whole pool.

## Orchestration (`internal/recommender`)

`Recommend(userID, n)` runs the whole thing:

1. **Load profile** from Redis (recent items, categories, price stats, embedding).
2. **Retrieve** the candidate pool (Stage 1, parallel).
3. **Filter already-seen** — cheaply, using the recently-viewed set. (A complete
   "never show anything they've ever interacted with" filter would need a per-user
   bloom filter over all history; we keep it to the hot set to protect the latency
   budget. Documented as a deliberate simplification.)
4. **Fill the popularity feature** for all candidates in a single `ZMScore` call
   (one round-trip, not N).
5. **Rank** (Stage 2).
6. **Diversify** the top pool with MMR → final top-n.
7. **Log** the served list asynchronously (best-effort) for CTR/offline eval —
   off the request's critical path.

Everything the request touches is pre-computed state, so the pipeline is a series
of fast reads plus light math — which is exactly how you hit tens-of-milliseconds
serving. The next docs wire this into an HTTP API with Prometheus metrics
(docs/08) and measure its quality offline (docs/09).
