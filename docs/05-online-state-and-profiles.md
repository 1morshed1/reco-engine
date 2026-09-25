# 05 — Online State: Redis Structures & Profile Updates

This doc covers `internal/store/redisstore` and `internal/profile`. It's about
the **write path**: how a stream of raw events becomes the small, fast, derived
state the request path reads. The guiding rule (docs/02): Redis holds *derived*
state — lossy, fast, rebuildable — never truth.

## Why Redis, and which structure for what

The request path must read user state and item stats in ~O(1). Redis is an in-
memory data-structure server, so we pick the native structure that makes each
read cheap:

| State                    | Redis structure | Key            | Why |
|--------------------------|-----------------|----------------|-----|
| Recently viewed items    | LIST            | `recent:{u}`   | LPUSH + LTRIM = a bounded most-recent-first list, O(1) |
| Category preferences     | HASH            | `catcount:{u}` | HINCRBYFLOAT accumulates weighted counts per field |
| Price mean/std           | HASH            | `price:{u}`    | store sum, sumsq, count → derive mean & std on read |
| User embedding           | STRING (JSON)   | `emb:{u}`      | one blob, GET/SET |
| Co-occurrence "also viewed" | ZSET         | `cooc:{item}`  | score = co-count; ZREVRANGE gives top-k instantly |
| Trending (global/cat)    | ZSET            | `trending[:c]` | score = time-decayed popularity; ZREVRANGE = top-k |

The sorted set (ZSET) is the star: "top-k trending" and "top-k co-viewed" are
exactly what a ZSET ranked by score gives you in `O(log n + k)`. That's why
trending and co-occurrence are essentially free at read time — the ranking is
maintained on write.

## Signal weighting (why not just +1)

Every update is weighted by `EventType.Weight()` — purchase 3.0, rating 2.0,
click 1.5, view 1.0. A user who *bought* ten action movies is a far stronger
"action fan" than one who *glanced* at ten. Counting raw events would blur that;
weighting preserves intent strength through the whole derived state.

## The per-event fan-out (`profile.Updater.Handle`)

For each event the updater does, in order:

1. **Append to the durable log** (Postgres `interactions`). Even unknown items
   are logged — history is complete regardless of derived state.
2. **Co-occurrence first**, against the user's *current* recent items (before
   adding this one), so an item never co-occurs with itself. Both directions get
   incremented (co-occurrence is symmetric).
3. **Recently viewed**: LPUSH the item, LTRIM to `recentN`.
4. **Category counter**: `+weight` on the item's category.
5. **Price accumulators**: fold the item's price into sum/sumsq/count.
6. **Interaction counter**: `+1`.
7. **Embedding**: weighted running average (below).
8. **Trending**: add a time-decayed weight (below).

### Weighted running-average embedding

The user vector should drift toward what they engage with, weighted by signal:

```
new = (old * W + itemEmb * w) / (W + w)     then L2-normalize
```

`W` is the accumulated weight (stored in `meta:{u}.embweight`). This is an
incremental mean — no need to re-read the user's whole history each event. A
purchase pulls the vector harder than a view. Re-normalizing keeps it on the unit
sphere so it stays comparable to item vectors for ANN.

### Time-decayed trending (the exponential-increment trick)

We want trending to favor *recent* popularity, but rewriting every score on a
timer is expensive. The trick: instead of decaying old scores, we **inflate new
increments**:

```
increment = weight * exp(age_seconds / tau)      tau = halfLife / ln2
age_seconds = (event_time - fixed_epoch)
```

Because every increment is scaled by `exp(t/tau)` and that factor only grows,
newer events contribute exponentially more than older ones. Ranking by raw score
therefore *is* ranking by time-decayed popularity — and we never touch old
entries. This is the classic Hacker News / Reddit ranking approach.

**Caveat (documented on purpose):** `exp(t/tau)` grows without bound, so over a
very long uptime the scores get astronomically large and eventually lose float
precision. For a demo/interview this is fine (float64 has enormous range). The
production fix is periodic rescaling (divide all scores by a common factor) or
resetting the epoch — the batch layer's natural home.

## Reading a profile (`GetProfile`)

On each request the API assembles a `UserProfile` from these keys:
- LRANGE recent → `RecentlyViewed`
- HGETALL catcount → counts → top-N → `PreferredCategories`
- price accumulators → `AvgPrice`, `PriceStd` (`std = sqrt(sumsq/n − mean²)`)
- GET emb → `Embedding`
- meta → `InteractionCount`

A brand-new user simply gets an empty-but-valid profile (all zero values) — the
retrieval stage handles that gracefully via trending/fresh sources (cold start,
docs/06–07).

## In-memory item cache in the processor

`profile.Updater` loads the whole catalog (metadata + embeddings) into memory at
startup so per-event handling needs no DB lookup for item fields. At 2000 items
that's trivial. In production this is where a **feature store** (Project F) or a
Redis-cached item table would sit; the seam is the `items`/`embs` maps.
