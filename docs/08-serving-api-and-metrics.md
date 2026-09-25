# 08 — Serving: The HTTP API and Metrics

This doc covers `cmd/api` and `internal/metrics` — how the pipeline is exposed
over HTTP and how we observe it. It also records a real bug we hit and fixed,
because that's the most instructive part.

## The endpoints

```
GET  /api/v1/recommend/:user_id?n=20   ranked, diverse recommendations
POST /api/v1/click  {user_id,item_id}  record a click (updates CTR)
GET  /healthz                          liveness probe
GET  /metrics                          Prometheus scrape target
```

`n` is parsed, defaulted (20), and capped (100) — never trust a client to bound
your work. Each handler runs under a **per-request timeout context** (2s) so a
slow dependency can't pin a connection open forever. The `recommend` handler
records metrics after serving; the `click` handler calls `MarkClicked`, which
flips the most recent served row for that (user, item) to `clicked = true`,
closing the feedback loop the offline evaluator reads.

Gin is in release mode with only the `Recovery` middleware — a panic in one
request returns 500 instead of taking down the server.

## What we measured (and it hit the target)

On warm state:
- **user 352** (a Romance/SciFi latent fan) → all-SciFi recommendations, from the
  `embedding` source. Personalization demonstrably works.
- **latency**: ~10ms warm, ~2ms cold — comfortably under the 50ms p99 budget.
- **cold user 99999** (no profile) → still gets results (pool of 80 from
  trending + fresh). Graceful cold start, no error.

The latency is low because — as designed — the request path only *reads
pre-computed state* and does light math. No training, no full-catalog scan.

## The metrics (`internal/metrics`)

Four instruments, matching the plan's observability goals:

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `recommendation_latency_seconds` | histogram | serving latency; buckets tuned around 50ms so p99 is readable |
| `recommendation_requests_total{status}` | counter | throughput and error rate |
| `recommendation_candidate_pool_size` | histogram | Stage-1 recall proxy; a shrinking pool means a starved source |
| `recommendation_profile_hits/misses_total` | counters | warm vs cold users — a cache-hit-rate proxy |

Prometheus (in the compose stack) scrapes `/metrics` every 5s via the docker
host gateway. From there it's one PromQL step to a p99 latency graph:
`histogram_quantile(0.99, rate(recommendation_latency_seconds_bucket[1m]))`.

## The bug we found by running it (important lesson)

The first live response had **scores around 146** and the ranking looked
suspicious. Cause: the ranker's popularity term was `log1p(rawTrendingScore)`,
and raw trending scores are on the exponential-increment decay scale — values
like `1e191`. `log1p(1e191) ≈ 440`, so the popularity term added a giant, nearly
constant offset that dwarfed every other feature. Ranking *order* mostly survived
(a constant offset doesn't reorder), but the feature was effectively broken and
the numbers were nonsense.

**Two fixes, both kept:**
1. **Normalize popularity within the candidate pool** to [0,1] before ranking
   (`recommender`). The absolute trending magnitude is meaningless — only order
   matters — so "popularity" now means *relative to this request's candidates*.
   Scores dropped to a sane ~2.3 and every feature contributes proportionally.
2. **Widen the trending half-life** default from 24h to 168h. The
   exponential-increment trick grows `exp(age/tau)` without bound; a larger `tau`
   pushes the float64-overflow horizon from ~2 years out to ~2 decades. (The
   *real* production fix is periodic rescaling in the batch layer — noted in
   docs/05.)

**The lesson:** a feature fed into a model/formula must be on a sane, bounded
scale. Raw counters that grow unboundedly (or live on an arbitrary internal
scale) have to be normalized before they mean anything as a feature. We only
caught this by running the thing and looking at the numbers — which is why every
phase here is built *and executed*, not just compiled.
