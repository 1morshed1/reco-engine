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
make migrate     # create the DB schema
make seed        # load the catalog + generate item embeddings
make processor   # (terminal 2) consume events, update Redis
make producer    # (terminal 3) simulate user activity into Kafka
make api         # (terminal 4) serve recommendations on :8080
```

Then:

```bash
curl 'http://localhost:8080/api/v1/recommend/1?n=10'
```

Offline quality metrics:

```bash
make eval        # precision@k, recall@k, nDCG, coverage
```

## Repository layout

```
cmd/         entrypoints: api, processor, seed, producer, eval
internal/    the engine: config, models, store/*, events, profile,
             candidates, ranking, diversity, recommender, metrics
migrations/  SQL schema
deploy/      prometheus config
docs/        the learning journal — read this
```

## Status

See the docs and git history; this is built and committed in phases, each with
its own learning doc.
