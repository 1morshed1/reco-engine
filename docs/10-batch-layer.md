# 10 — The Batch Layer (and why it isn't Airflow)

This doc covers `cmd/batch`. It's the **cold path** — work that runs
occasionally, isn't latency-sensitive, and corrects or refreshes the state the
hot path depends on. The plan calls for Airflow here; we deliberately used a
plain Go job instead (docs/01 D9) and this doc explains what it does and why.

## The three data planes, one more time

- **Hot path** (per request): read pre-computed state, serve in ~ms.
- **Streaming path** (per event): keep hot state fresh, continuously.
- **Cold/batch path** (nightly): recompute things properly from full history,
  fix drift, and evaluate quality.

The batch layer exists because some work is *wrong or impossible* to do
incrementally on the hot/streaming paths:

## What `cmd/batch` does

### 1. Recompute item embeddings
Rewrites every item's embedding from the catalog. Today the embedding function is
content-based, so this only matters if the function changes — but it's the exact
seam where a **real training job** lives: a nightly matrix-factorization or
two-tower model reading the `interactions` table would write the `items.embedding`
column here, and nothing else in the system would change (docs/03).

### 2. Rebuild co-occurrence from full history
The online processor maintains co-occurrence incrementally, but at-least-once
delivery means it can **double-count** on replay (docs/06). The batch job wipes
`cooc:*` and recomputes the matrix exactly from the complete interaction log,
mirroring the online rule (each event co-occurs with the items in the user's
recent window, weighted by signal strength). This is the classic **Lambda
architecture** pattern: a fast, approximate streaming layer for freshness + a
slow, exact batch layer that periodically corrects it.

Our run rebuilt **243,315** co-occurrence pairs from history — the authoritative
version that replaces whatever the streaming layer had accumulated.

### 3. Rescale trending scores (the overflow fix)
The trending decay uses the exponential-increment trick (docs/05), whose scores
grow without bound. The batch job divides **every** trending score by a common
factor (the current global max). Because all scores are divided equally, the
**ranking is exactly preserved** — but the magnitudes reset to a sane range
(our run divided by 1.65e28, so the top item's score became 1.0). This is the
production answer to the overflow caveat, and it belongs on a periodic job, not
the hot path.

### 4. Offline evaluation
The fourth nightly task is quality measurement. It lives in its own command
(`cmd/eval`, docs/09) so it can also be run ad hoc, but conceptually it's part of
the same nightly sweep: seed → serve all day → batch-correct + evaluate → repeat.

## Why not Airflow?

Airflow is a Python DAG scheduler/orchestrator: a scheduler, a metadata DB, a
webserver, workers. It earns its weight when you have *many* interdependent
pipelines, backfills, retries, and a team coordinating them. Here the nightly work
is **one linear job**. The valuable, learnable content is the *transforms* (the
three tasks above) — and those are plain Go. So:

```
cron:  0 3 * * *   /path/to/bin/batch
```

is the whole scheduler. We kept the machinery proportional to the problem and
documented exactly where Airflow would slot in if the job graph grew — each of
the three steps becomes a task, with dependencies (embeddings → co-occurrence →
eval) expressed as a DAG. The *code* wouldn't change; only the thing that invokes
it.

## Lambda architecture in one picture

```
        events
          │
   ┌──────┴───────┐
   ▼              ▼
STREAMING      (log to Postgres)
(fast, approx)      │
   │                ▼
   │            BATCH (nightly, exact)
   │                │
   ▼                ▼
  Redis  ◄── corrected by ──  batch
   │
   ▼
 serving (hot path reads Redis)
```

Fast layer for freshness, batch layer for correctness, both feeding the same
serving state. That's the shape worth remembering.
