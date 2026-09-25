# 01 — Architecture Decisions (with the tradeoffs)

This is an ADR-style log: each entry is a decision, the options we weighed, and
*why* we chose what we chose. In interviews, the "why" is what separates people
who've built systems from people who've read about them.

---

## D1 — Language: Go for the serving path

**Chosen:** Go.

**Why:** The serving path is I/O-bound (Redis, Postgres, Kafka) with a hard
latency budget and high concurrency. Go's goroutines make "fan out to 4
candidate sources in parallel, wait for all" a few lines of code, and its
scheduler handles thousands of concurrent requests without a thread-per-request
blowup. Compiled, GC-managed, predictable p99. Python would be simpler for the
ML math but far slower and clumsier for concurrent serving.

**Tradeoff:** The ML/analytics ecosystem (pandas, scikit-learn, XGBoost) lives
in Python. We accept that by keeping the *ranking model* simple (a weighted
formula) rather than a trained gradient-boosted tree. See D7.

---

## D2 — Event bus: Kafka via `segmentio/kafka-go`

**Chosen:** Kafka, client library `segmentio/kafka-go`.

**Why Kafka:** We need a durable, replayable log of user activity. Durability
means the processor can crash and resume; replayable means we can rebuild Redis
state or re-train from history by re-reading the topic. A plain queue (delete on
read) can't do that.

**Why segmentio over confluent-kafka-go:** `confluent-kafka-go` wraps the C
`librdkafka` and needs cgo + system libraries. `segmentio/kafka-go` is pure Go —
no cgo, trivial cross-compilation, simple API. For this project's scale the pure-
Go client is plenty fast.

**Tradeoff:** `librdkafka` is marginally faster and has more features
(transactions, exactly-once). We don't need those here.

---

## D3 — Kafka in KRaft mode (no ZooKeeper)

**Chosen:** Single-node Kafka in **KRaft** mode.

**Why:** Modern Kafka (3.x+) can run its own metadata quorum (KRaft) instead of
requiring a separate ZooKeeper cluster. One fewer container, one fewer thing to
understand, and it's the direction Kafka is going (ZooKeeper is being removed).

**Tradeoff:** Single node = no real fault tolerance. Fine for a dev/learning
stack; production would run 3+ brokers.

---

## D4 — Two stores, on purpose: Redis + Postgres/pgvector

**Chosen:** Redis for hot online state, Postgres (with pgvector) for the catalog,
logs, and embeddings/ANN.

**Why split them:**
- **Redis** is an in-memory data-structure server. Sorted sets give us trending
  (score = popularity, `ZREVRANGE` for top-N) essentially for free. Hashes give
  us user profiles. Everything the request path reads should be O(1)-ish and in
  memory — that's what keeps us under the latency budget.
- **Postgres** is the source of truth: durable item catalog, the full
  interaction log, and the recommendation log for evaluation. Relational queries
  and joins live here.
- **pgvector** is a Postgres extension that adds a `vector` column type and ANN
  indexes (IVFFlat / HNSW). It lets us do "find items similar to this embedding"
  with SQL, so we don't run a *separate* vector database.

**Why pgvector instead of a dedicated vector DB (Pinecone, Milvus, Qdrant):**
One fewer system to run and reason about; embeddings live next to the catalog so
a similarity search can join to metadata in one query. At millions of vectors
pgvector with HNSW is competitive. A billion-vector workload would justify a
dedicated store — we don't have one.

**Tradeoff:** Redis is not durable by default; if it dies we lose hot state. But
that state is *derived* — the processor can rebuild it by replaying Kafka, and
the batch job recomputes it from Postgres. So we treat Redis as a cache of
derived state, never as a source of truth.

---

## D5 — Embeddings: content-based, generated in-process

**Chosen:** Derive each item's embedding from its **content features** (genres,
plus normalized popularity/recency) with a small deterministic function, in-
process — no external embedding API.

**Why:** The learning goal is the *pipeline* (ANN retrieval, cosine similarity,
ranking, diversity), not owning a GPU. Content-based embeddings built from genre
vectors are genuinely meaningful: two sci-fi movies land near each other in
vector space, so cosine similarity and ANN search demonstrate real behavior. And
the whole system runs offline with zero API keys or cost.

**Tradeoff vs. a real model:** A production system would learn embeddings from
the interaction matrix (collaborative filtering — matrix factorization / two-
tower neural nets) or embed rich text with a transformer. Those capture "users
who like A also like B" signals that pure content can't. We get the collaborative
signal a *different* way — through the co-occurrence source in retrieval (D6) —
so the overall system still blends content and collaborative signals. The
embedding function is isolated in one file, so swapping in real embeddings later
is a drop-in change.

See `docs/03-embeddings-and-ann.md` for the exact construction.

---

## D6 — Retrieval sources: blend content + collaborative + popularity + fresh

**Chosen:** Four parallel candidate sources — ANN (content), co-occurrence
("also viewed", collaborative), trending (popularity), and fresh items
(exploration).

**Why multiple sources:** Any single source has a failure mode. Pure content
recommends more of the same (filter bubble). Pure collaborative can't handle new
users/items (cold start). Pure popularity ignores personalization. Blending
sources covers each other's blind spots and is exactly how real systems build
their candidate pool. Running them concurrently keeps the total time ~= the
slowest single source, not the sum.

---

## D7 — Ranking: weighted formula first, model-ready shape

**Chosen:** A transparent weighted-sum ranker over per-candidate features
(embedding similarity, popularity, recency, price/rating match, category match,
source boost).

**Why:** It's inspectable (you can read off why an item ranked where), needs no
training data or serving infra, and — critically — it's shaped like a real
model: it consumes a **feature vector per candidate**. Replacing the weighted sum
with a trained XGBoost/neural scorer later means swapping the `Score` function,
not rebuilding the pipeline. Start simple, keep the seams.

**Tradeoff:** Hand-tuned weights are worse than a model trained on real click
data. We expose the weights as config so they can be tuned, and the eval harness
(D9) lets us measure whether a change helped.

---

## D8 — Diversity: MMR post-processing

**Chosen:** Maximal Marginal Relevance to re-rank the top ranked items.

**Why:** Ranking alone converges on near-duplicates (five movies from the same
franchise). MMR picks the next item by balancing *relevance* against *novelty*
(dissimilarity to what's already chosen), controlled by a single `lambda`. It's
the standard, well-understood diversity primitive and it reuses the embeddings we
already have (novelty = cosine distance in embedding space).

---

## D9 — Batch layer: a Go job, not Airflow

**Chosen:** Implement the "nightly batch" (recompute embeddings, co-occurrence,
popularity; run offline evaluation) as a Go command you can run on a cron,
instead of standing up Airflow.

**Why:** Airflow is a Python scheduler/orchestrator — valuable when you have many
interdependent DAGs and a team, but it's a heavyweight dependency (its own DB,
scheduler, webserver) that adds nothing to the *learning* here. The actual work
(the transforms and metrics) is what matters, and that's plain Go. We document
where Airflow *would* slot in so the production shape is clear.

**Tradeoff:** No web UI, no retry/backfill orchestration, no DAG visualization.
For a single linear nightly job, `cron + a binary` is the right amount of
machinery.

---

## D10 — Config via environment variables

**Chosen:** All connection strings and tunables come from env vars with sane
defaults (see `internal/config`), matching `docker-compose.yml`.

**Why:** 12-factor style. A fresh clone runs with zero config because the
defaults point at the local compose stack; production overrides via the
environment. No config files to parse, no secrets in the repo.
