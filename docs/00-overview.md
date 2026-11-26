# 00 — Overview: What We're Building and Why

> This `docs/` folder holds the **design notes**. Each file explains not just
> *what* the code does, but *why* it's built that way and what the production
> alternatives are. Read them in numeric order. They are written to be useful
> even if you never touch the code.

## The one-sentence version

A **real-time recommendation engine**: user activity events stream in through
Kafka, get turned into live user profiles and item statistics in Redis, and a
Go API answers `GET /recommend/:user_id` in ~tens of milliseconds by running a
**two-stage retrieval → ranking pipeline** over a movie catalog stored in
Postgres with vector (embedding) search via pgvector.

## Why this project matters

"Design a recommendation system" is the single most common ML **system design**
interview question. Most people can describe collaborative filtering in a
notebook. Very few have built the part that's actually hard: **serving** —
generating candidates, ranking them, and returning results under a latency
budget while the user waits.

This project builds that serving path end to end. When you're done you can point
at a running system instead of describing one on a whiteboard.

## The mental model: two stages

You cannot score every item in a catalog for every request. A catalog can have
millions of items; a request has a ~50ms budget. So every large-scale
recommender splits the work in two:

```
                 millions of items
                        │
        ┌───────────────▼───────────────┐
        │  STAGE 1: CANDIDATE RETRIEVAL  │   cheap, wide net, imprecise
        │  (embeddings, co-occurrence,   │   pull ~200 plausible items
        │   trending, fresh items)       │   from several sources in parallel
        └───────────────┬───────────────┘
                        │  ~200 candidates
        ┌───────────────▼───────────────┐
        │  STAGE 2: RANKING              │   expensive per item, precise
        │  (feature-rich score per item) │   score all 200, sort
        └───────────────┬───────────────┘
                        │  top ~40
        ┌───────────────▼───────────────┐
        │  POST-PROCESSING               │   dedup, diversity (MMR),
        │  (diversity, business rules)   │   business rules → final top-N
        └───────────────┬───────────────┘
                        │
                    top-N result
```

Retrieval trades precision for speed and recall (don't miss good items).
Ranking trades speed for precision (get the order right on a small set). This
division is the single most important idea in the whole project — Netflix,
YouTube, and Amazon all work this way.

## The two data planes

There are two very different jobs, and they use different stores:

- **Online / hot path** (must be fast, read on every request): Redis holds user
  profiles, co-occurrence counts, and trending scores. Postgres+pgvector holds
  embeddings for ANN search. This path is what the latency budget applies to.
- **Streaming / write path** (keeps the hot state fresh): Kafka carries events,
  the processor consumes them and updates Redis. This runs continuously and
  independently of requests.
- **Batch / cold path** (runs occasionally, not latency-sensitive): recompute
  embeddings and co-occurrence from full history, evaluate quality offline.

Keeping these separate is why the system stays fast: the request path never does
heavy computation, it only *reads pre-computed state*.

## Components (and the code that implements each)

| Component            | Job                                              | Code |
|----------------------|--------------------------------------------------|------|
| Event producer       | Simulate user activity → Kafka                   | `cmd/producer` |
| Event processor      | Consume events → update Redis profiles/stats     | `cmd/processor`, `internal/profile` |
| Recommendation API   | Serve the two-stage pipeline over HTTP           | `cmd/api`, `internal/recommender` |
| Candidate retrieval  | Pull ~200 candidates from 4 sources in parallel  | `internal/candidates` |
| Ranking              | Score each candidate with a weighted formula     | `internal/ranking` |
| Diversity            | MMR re-ranking to avoid filter bubbles           | `internal/diversity` |
| Vector store         | pgvector ANN search over item embeddings         | `internal/store/vector` |
| Catalog / logs       | Item metadata + interaction/reco logs            | `internal/store/postgres` |
| Online state         | Profiles, co-occurrence, trending                | `internal/store/redisstore` |
| Metrics              | Prometheus latency/QPS/cache-hit                 | `internal/metrics` |
| Offline eval         | precision@k, recall@k, nDCG, coverage            | `cmd/eval` |

## Latency budget (the target we design against)

- End-to-end p99: **under 50ms**. The pipeline must read pre-computed state and
  do only light math per candidate.
- Throughput: **thousands of QPS** on a single Go instance — achievable because
  Go handles concurrency cheaply and the request path does no heavy work.

## What's next

- `01-architecture-decisions.md` — the choices we made and the tradeoffs.
- Then the numbered docs follow the build order: data model, embeddings, Redis
  state, streaming, retrieval, ranking, diversity, serving, evaluation.
