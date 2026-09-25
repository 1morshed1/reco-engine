# Real-Time Recommendation Engine (Go + Kafka + Redis + pgvector)

A production-shaped recommendation **serving** system: user activity streams in
through Kafka, becomes live user profiles and item statistics in Redis, and a Go
API answers recommendation requests in tens of milliseconds using a two-stage
**retrieval → ranking** pipeline over a movie catalog in Postgres with pgvector
ANN search.

Built as a learning project. The `docs/` folder is a full learning journal that
explains every decision — **start there** (`docs/00-overview.md`).

## Why it exists

"Design a recommendation system" is the most common ML system-design interview
question, and almost nobody has built the hard part: real-time serving
(candidate generation, ranking, diversity) under a latency budget. This is that
part, running end to end.

## Architecture at a glance

```
events ──▶ Kafka ──▶ Processor ──▶ Redis (profiles, co-occurrence, trending)
                                      │
   HTTP request ──▶ API ──▶ [retrieval → ranking → diversity] ──▶ top-N
                              │            │
                     pgvector ANN     Postgres catalog + logs
```

Two stages, because you can't score a whole catalog per request:
1. **Retrieval** — pull ~200 candidates from 4 sources in parallel (embeddings,
   "also viewed", trending, fresh). Fast, wide, imprecise.
2. **Ranking** — score those 200 with a feature-rich formula. Slow, narrow,
   precise. Then MMR diversity → final top-N.

## Tech choices (full rationale in `docs/01`)

| Concern        | Choice                          |
|----------------|---------------------------------|
| Serving        | Go (goroutine fan-out, low p99) |
| Event bus      | Kafka (KRaft), `segmentio/kafka-go` |
| Online state   | Redis (sorted sets, hashes)     |
| Catalog + ANN  | Postgres + pgvector             |
| Metrics        | Prometheus                      |
| Batch/eval     | Go command on cron (not Airflow)|

## Quick start

Prereqs: Docker + Docker Compose, Go 1.24+.

```bash
make up          # start postgres, redis, kafka, prometheus
make tidy        # resolve Go dependencies
make seed        # apply schema, load catalog, embeddings, ANN index, history
```

Then, in separate terminals, run the streaming + serving stack:

```bash
make processor   # terminal 2: consume events -> update Redis
make producer    # terminal 3: simulate live user activity into Kafka
make api         # terminal 4: serve recommendations on :8080
```

Query it:

```bash
curl 'http://localhost:8080/api/v1/recommend/42?n=10'      # recommendations
curl -XPOST localhost:8080/api/v1/click \
  -H 'Content-Type: application/json' -d '{"user_id":42,"item_id":1156}'
curl localhost:8080/metrics                                 # prometheus metrics
```

Offline quality, the nightly batch job, and a load test:

```bash
make eval        # precision@k, recall@k, nDCG, coverage (3 strategies compared)
make batch       # recompute embeddings, rebuild co-occurrence, rescale trending
make loadtest    # throughput + latency percentiles
make test        # unit tests
```

## Results

- **Quality** (offline, leakage-free eval): personalized strategies beat the
  popularity baseline on precision/recall/nDCG; the full pipeline has the best
  nDCG and **~30x the catalog coverage** of popularity (0.21 vs 0.007 — it escapes
  the filter bubble). See `docs/09`.
- **Latency**: the pipeline serves at **p50 ~4ms, p99 ~8.5ms** — well under the
  50ms target. (Under a single-laptop-shared-with-all-services load test at c=50,
  p99 rises to ~60ms due to CPU contention, not the pipeline; see `docs/11`.)
- **Personalization works**: a SciFi-leaning user gets SciFi recommendations; a
  brand-new user is served gracefully from trending/fresh (cold start).

## Repository layout

```
cmd/         entrypoints: api, processor, seed, producer, eval, batch, loadtest
internal/    the engine: config, models, store/{postgres,redisstore},
             catalog, embedding, dataset, events, profile, candidates,
             ranking, diversity, recommender, metrics
migrations/  SQL schema (embedded)
deploy/      prometheus config
docs/        the learning journal — read docs/README.md first
```

## Status

Complete and runnable. Built in phases, each with its own learning doc in
`docs/` and its own commit. The `docs/` folder is the primary deliverable — it
explains every decision so the codebase doubles as a course.
