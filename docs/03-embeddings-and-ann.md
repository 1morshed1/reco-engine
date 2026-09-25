# 03 — Embeddings and ANN Search

This doc covers `internal/embedding` and the pgvector similarity search in
`internal/store/postgres`. It answers: what is an embedding here, how do we build
one without an ML model, and how does "find similar items" work fast.

## What an embedding is (in this project)

An **embedding** is a fixed-length vector of numbers that places an item in a
geometric space where *distance means dissimilarity*. If two movies are close,
they're similar; if far, they're different. Once items live in such a space,
"recommend things like what you liked" becomes "find the nearest vectors" — a
geometry problem with fast, well-known algorithms.

## How we build item vectors (no model required)

See `embedding.ItemVector`. The vector is assembled from content:

```
dims [0 .. len(Genres))   multi-hot genre membership   (1.0 in each genre's slot)
dim   len(Genres)+0       recency  (year normalized ~0..1)
dim   len(Genres)+1       rating   (0..5 normalized to 0..1)
... remaining dims are zero-padded up to config.EmbeddingDim (32)
then the whole vector is L2-normalized (scaled to unit length)
```

Because genres dominate the vector, two sci-fi movies end up pointing in nearly
the same direction, so their cosine similarity is high. We verified this: the
nearest neighbors of a Drama movie are other Drama movies. That's the whole point
— the geometry encodes real similarity, with zero training.

### Why L2-normalize?

After normalization every vector has length 1. Then **cosine similarity = dot
product**, and cosine distance behaves cleanly for the ANN index. It also means
"how similar" depends only on *direction* (which genres), not *magnitude* (how
many genres an item happens to list).

### The honest tradeoff (see docs/01 D5)

Content embeddings capture "similar *content*" but not "users who liked A also
liked B" (collaborative signal). We recover that separately via the co-occurrence
retrieval source (docs/05). A production system would *learn* embeddings from the
interaction matrix so a single vector captures both. Because all vector
construction is isolated in `ItemVector`, swapping to learned vectors is a
one-file change — the schema, the ANN query, and everything downstream stay
identical.

## The user vector

A user's embedding (`UserProfile.Embedding`) is the **L2-normalized average of the
embeddings of items they interacted with** (`embedding.Average`). Averaging unit
vectors and re-normalizing yields a direction that points toward the user's
center of taste. Since users and items live in the *same* space, "items near the
user" (ANN) is exactly "items matching the user's taste."

Signal weighting note: the processor can weight the average by event strength
(a purchase counts more than a view) — see docs/05.

## ANN: Approximate Nearest Neighbor search

Finding the exact nearest vectors means comparing the query to *every* item —
O(N) per request. Too slow at scale. **ANN** trades a little accuracy for a lot
of speed using an index.

### pgvector + IVFFlat

pgvector adds a `vector` column type and distance operators to Postgres:
- `<=>` cosine distance, `<->` L2 distance, `<#>` negative inner product.

We use the **IVFFlat** index with `vector_cosine_ops`:

```sql
CREATE INDEX idx_items_embedding ON items
USING ivfflat (embedding vector_cosine_ops) WITH (lists = N);
```

**How IVFFlat works:** it clusters all vectors into `lists` buckets (like
k-means centroids). A search only scans the buckets nearest the query instead of
the whole table. `lists ≈ sqrt(rows)` is the usual starting point (we compute it
in `CreateANNIndex`). More lists = finer buckets = faster search but lower recall
per probe.

**Why the index is built *after* seeding (a real gotcha):** IVFFlat learns its
cluster centroids from existing data. Build it on an empty table and the clusters
are meaningless, wrecking recall. So `cmd/seed` inserts items + embeddings
*first*, then creates the index. This ordering is deliberate and documented in
the schema and the seeder.

### The search query

```sql
SELECT ..., (1 - (embedding <=> $1::vector)) AS similarity
FROM items WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1::vector
LIMIT $2;
```

`embedding <=> $1` is cosine *distance* (0 = identical, 2 = opposite). We return
`1 - distance` as a `similarity` in ~[−1, 1] (≈1 = very similar) so ranking can
use it directly. `ORDER BY <=> LIMIT k` is what the index accelerates.

### IVFFlat vs HNSW

pgvector also offers **HNSW** (a graph index): better recall/latency, higher
memory and slower build. For this dataset IVFFlat is simpler and fine. Switching
is a one-line index change — the query operators are identical.

## Why store vectors as text on the wire

The store encodes vectors as pgvector's text literal `"[a,b,c]"` and casts with
`::vector`, and parses the same form back (`vecToString`/`parseVec`). This avoids
depending on a binary client codec — the SQL stays explicit and the dependency
list stays tiny. At this scale the text encoding cost is negligible.
