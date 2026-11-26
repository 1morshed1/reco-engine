# Design Notes — Read in Order

These notes explain how this real-time recommendation engine is built,
decision by decision. Each doc explains
the *why*, the *tradeoffs*, and the *production alternatives* — not just the code.
Read them in order; each builds on the last.

| # | Doc | What it covers |
|---|-----|-------------------|
| 00 | [Overview](00-overview.md) | The whole system in one picture: the two-stage (retrieval → ranking) pipeline, the three data planes, the latency budget. **Start here.** |
| 01 | [Architecture Decisions](01-architecture-decisions.md) | Every major choice as an ADR: Go, Kafka/KRaft, Redis+pgvector, content embeddings, weighted ranking, MMR, batch-vs-Airflow — with the tradeoffs. |
| 02 | [Data Model](02-data-model.md) | Events, items, profiles, logs — and the crucial split between *derived hot state* (Redis) and the *durable log* (Postgres). |
| 03 | [Embeddings & ANN](03-embeddings-and-ann.md) | What an embedding is, how we build one with no ML model, cosine similarity, and how pgvector/IVFFlat makes "find similar" fast (plus the index-after-load gotcha). |
| 04 | [Synthetic Data](04-synthetic-data.md) | Why we generate a world with hidden tastes — it gives us *ground truth* to evaluate against, reproducibly and offline. |
| 05 | [Online State & Profiles](05-online-state-and-profiles.md) | Redis data-structure choices, signal-weighted updates, the running-average user embedding, and the exponential-increment trending-decay trick. |
| 06 | [Streaming (Kafka)](06-streaming-kafka.md) | Why a log beats a queue, partitioning for per-user ordering, consumer groups, at-least-once delivery and what it means for updates. |
| 07 | [Retrieval → Ranking → Diversity](07-retrieval-ranking-diversity.md) | The core pipeline in detail: 4 parallel candidate sources, the weighted ranking formula, and MMR diversity. |
| 08 | [Serving API & Metrics](08-serving-api-and-metrics.md) | The HTTP endpoints, Prometheus metrics, and a real bug we found by running it (popularity feature on the wrong scale). |
| 09 | [Evaluation](09-evaluation.md) | Leakage-free offline eval: temporal train/test split, precision/recall/nDCG/coverage, and how personalization beats the popularity baseline. |
| 10 | [Batch Layer](10-batch-layer.md) | The nightly cold path (embeddings, exact co-occurrence rebuild, trending rescale), the Lambda architecture, and why not Airflow. |
| 11 | [Benchmarks & Performance](11-benchmarks-and-performance.md) | Load-test results, how we found the *real* bottleneck (and why two optimizations barely helped), and how the design scales. |

## The single most important idea

If you take one thing from this project, take the **two-stage architecture**:

> You can't score a whole catalog per request. So **retrieve** a wide, cheap pool
> of candidates from several sources, then **rank** that small pool precisely, then
> **diversify**. Everything the request touches is pre-computed state, so serving
> is fast.

That pattern — and the discipline of separating the hot (serve), streaming
(update), and batch (correct) data planes — is how Netflix, YouTube, and Amazon
recommendations actually work.
