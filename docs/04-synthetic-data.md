# 04 — Synthetic Data: Why We Generate a World

This doc covers `internal/dataset` and `cmd/seed`. The key idea: because we
*define* each user's hidden taste, we get **ground truth** to evaluate against —
something you don't get for free with a raw downloaded dataset.

## The problem with just downloading MovieLens

MovieLens is real and great, but: (a) it needs a download step (breaks "runs
offline"), and (b) you only see what users *did*, never *why*. To measure whether
a recommender is good, you hold out some interactions and check if it predicts
them — that works, but you can't inspect the underlying preference.

By generating the world, we control the hidden preference model, so we can reason
precisely about whether the system recovers it. And it's fully reproducible
(seeded RNG) — two runs produce identical data, so experiments are comparable.

## The generative model

### Items (`Catalog`)
Each of ~2000 items gets:
- 1–3 genres drawn from the fixed vocabulary,
- a year in 1980–2024,
- a rating ~ Normal(3.5, 0.7) clamped to [1, 5],
- a price correlated with recency + rating plus noise (so **price-match** ranking
  has real structure to exploit),
- a `created_at` spread across the recent past (so **fresh/recency** signals have
  a gradient).

### Users (`Users`) — the hidden taste
Each of ~500 users has a **latent** profile the recommender never sees:
- `FavGenres`: 1–3 genres they actually like,
- `PriceMean` / `PriceStd`: their spending comfort zone.

### Affinity (the ground-truth "how much would this user like this item")
```
affinity = 2.0 * genreOverlapFraction        # taste match dominates
         + 0.5 * gaussianPriceFit             # in their price zone?
         + 0.3 * (itemRating / 5)             # is it objectively good?
```
This is the signal a good recommender should recover *without being told it*.

### Interactions (`GenerateInteractions`)
For each user we build a 200-item shortlist and sample from it with probability
∝ `exp(affinity)` (a softmax) — so users mostly interact with items they'd like,
but with realistic noise. Each interaction becomes an event with a realistic type
mix:

```
60% view, 20% click, 12% rating (rated ~ affinity), 8% purchase
```

Events are timestamped within the last `HistoryDays` and sorted chronologically,
so replaying them mimics a real arrival stream.

## Performance note (why the shortlist)

Scoring all 2000 items for every one of 20,000 interactions would be O(users ×
events × items). Instead each user samples from a fixed 200-item shortlist with
precomputed weights — O(users × (shortlist + events)). Same statistical flavor,
far faster seeding (the whole seed runs in well under a second).

## What seeding produces

```
make seed
→ 2000 items with embeddings, IVFFlat index built
→ 500 users
→ 20000 interactions in the log
```

This is enough for:
- **retrieval** to have a real catalog and co-occurrence structure,
- **ranking** to have prices/ratings/recency to weigh,
- **evaluation** to hold out interactions and measure precision/recall/nDCG
  against the taste model that produced them.

## Where this maps to reality

In production, `internal/dataset` is replaced by real event ingestion: the
catalog comes from your product DB, and interactions come from real users via
Kafka. The *shapes* (Item, Event) are identical, so nothing downstream changes.
The generator is purely a stand-in so the whole system is runnable and testable
today.
