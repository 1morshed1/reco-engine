# 02 — Data Model: Events, Items, Profiles, Logs

This doc explains the domain types (`internal/models`) and the Postgres schema
(`migrations/001_schema.sql`), and — more importantly — *why the data is split
the way it is*.

## The four core records

### 1. Event — the atom of activity
```
Event{ UserID, ItemID, Type, Value, Timestamp }
```
An `Event` is one thing a user did: a view, click, purchase, or rating. It is the
only *input* to the whole online system. Everything else (profiles, trending,
co-occurrence) is **derived** from a stream of events.

`EventType.Weight()` encodes a key idea: **not all signals are equal**. A
purchase (3.0) tells you far more about taste than a passing view (1.0). When we
update preference counters, we add the event's weight, not just `+1`. This is the
difference between "you glanced at this" and "you bought this."

### 2. Item — the catalog entry (source of truth)
```
Item{ ID, Title, Category, Genres[], Year, Price, Rating, CreatedAt }  + embedding
```
Durable, authored data. `Category` is the primary genre (indexed, for fast
filtering); `Genres[]` is the full list and drives the content embedding.
`Price` is synthetic — it exists so we can demonstrate **price-match** ranking
(recommend things in the user's usual spending range).

### 3. UserProfile — the derived, hot view of a user
```
UserProfile{ RecentlyViewed[], PreferredCategories[], CategoryCounts,
             AvgPrice, PriceStd, Embedding[], InteractionCount }
```
This is what the request path reads on **every** recommendation. It never gets
authored directly — the processor rebuilds it from events. Two things worth
noting:
- `Embedding` is a **running average of the embeddings of items the user
  interacted with**. That places the user in the same vector space as items, so
  "items near the user" is a meaningful ANN query.
- `PriceStd` lets ranking treat price-match as a Gaussian: items near the user's
  typical price get boosted, and a user with erratic spending (high std) is
  penalized less for price mismatch.

### 4. Logs — interactions and recommendations
- `interactions` is an **append-only** copy of every event, in Postgres. Redis
  holds the *summary* (fast, lossy, derived); Postgres holds the *full history*
  (durable, complete). The batch layer and the offline evaluator both read this.
- `recommendations` records what we served (user, item, rank, source, score) and
  whether it was clicked. This closes the loop: it's how we measure CTR and
  compare strategies online.

## Why derived state (Redis) *and* full log (Postgres)?

This is the crux of the design and a classic interview point:

| Question                         | Redis (derived) | Postgres (log) |
|----------------------------------|-----------------|----------------|
| Read on every request?           | Yes, must be fast | No |
| Complete history?                | No (summary/recent) | Yes |
| Survives a crash?                | Rebuildable from Kafka/PG | Durable |
| Used for training/eval?          | No | Yes |

The request path reads only the fast, derived summary. The heavy, complete data
sits in Postgres where latency doesn't matter. If Redis is wiped, we lose nothing
permanent — the processor replays Kafka and/or the batch job recomputes from the
`interactions` table. **Treat derived state as a cache; never as truth.**

## Schema notes

- **`embedding vector(32)`** on the `items` row. The dimension (32) must match
  `config.EmbeddingDim`. Co-locating the embedding with the item means an ANN
  search can `JOIN`/return metadata in the same query — no cross-store fan-out
  for the similarity source.
- **The IVFFlat ANN index is created by `cmd/seed`, not in the base schema.**
  IVFFlat clusters existing vectors to build its lists; building it on an empty
  table produces a bad index. So we insert rows + embeddings first, then create
  the index. (Documented again in `docs/03`.)
- **Indexes** target the actual access patterns: `interactions(user_id, ts DESC)`
  for "this user's recent activity", `items(category)` and `items(created_at)`
  for the trending/fresh retrieval sources.

## Where the ML would plug in

Right now embeddings are content-based and computed in `cmd/seed`. In a
production system the `items.embedding` column would be written by a training
job (matrix factorization / a two-tower model reading the `interactions` table),
and `UserProfile.Embedding` might come from the same model rather than a running
average. The *schema doesn't change* — only who writes the vectors. That's the
point of keeping the embedding a plain column.
