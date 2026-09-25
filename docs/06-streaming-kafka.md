# 06 — Streaming: Kafka, the Processor, and the Producer

This doc covers `internal/events`, `cmd/processor`, and `cmd/producer` — the
pieces that move user activity from "something happened" to "derived state is
updated." It's the beating heart of a *real-time* recommender.

## Why a log, not a queue

We could POST events straight to the processor. We use Kafka instead because a
**durable, replayable log** buys us three things a plain queue can't:

1. **Decoupling** — producers don't care if the processor is up, slow, or being
   redeployed. Events pile up safely in the log.
2. **Replay** — the consumer group can re-read from the start. That's how a fresh
   stack warms an empty Redis: the processor replays existing history. It's also
   how you'd rebuild derived state after a bug or a schema change.
3. **Fan-out** — tomorrow, a fraud detector or an analytics job can consume the
   same topic independently, each with its own offset.

## The wire format and partitioning (`internal/events`)

Events are JSON `models.Event`. The producer keys each message by **user ID**:

```go
Balancer: &kafka.Hash{}   // partition = hash(userID) % partitions
Key:      userID
```

**Why key by user:** all of one user's events land on the same partition, and a
partition is ordered. That matters because profile updates are order-sensitive —
the recently-viewed list and the running-average embedding assume events arrive
in the order they happened. Keying by user gives per-user ordering without the
cost of global ordering (which would force a single partition and kill
throughput).

`AllowAutoTopicCreation: true` lets the first write create the topic, so there's
no separate setup step (the broker has auto-create on). On a cold broker the very
first few writes can race topic creation and retry — harmless.

## The consumer group (`cmd/processor`)

```go
kafka.NewReader({ GroupID: "reco-processor", StartOffset: FirstOffset })
```

- **GroupID** means Kafka tracks committed offsets for us, and lets you run
  multiple processor instances that split the partitions between them (horizontal
  scaling). Add instances → each takes a share of partitions.
- **`ReadMessage` auto-commits** the offset after a successful read. That gives
  **at-least-once** delivery: if the processor crashes mid-update, the event is
  re-read and re-applied on restart.

### At-least-once ⇒ updates should tolerate replays

Because an event can be applied more than once, ask: is each update safe to
repeat? Mostly yes, and where not, it's bounded:
- Recently-viewed: a duplicate just re-inserts an item that ages out — harmless.
- Category/price/trending counters: a duplicate slightly over-counts. For a
  recommender (a *statistical* system, not a ledger) this is acceptable noise;
  the batch layer recomputes exact values from the log periodically.
- The durable `interactions` insert is the one place double-apply adds a real
  duplicate row; the offline evaluator de-dupes by `(user, item)` where it
  matters.

If we needed exactly-once we'd track processed offsets transactionally with the
side effects — far more machinery than a recommender warrants.

### Graceful shutdown

The processor runs off a `signal.NotifyContext` (SIGINT/SIGTERM). On a signal the
read loop's context cancels, `Read` returns `context.Canceled`, and we exit after
logging the count. Each event's `Handle` gets its own short timeout context so an
in-flight update can finish cleanly rather than being torn off.

## The producer (`cmd/producer`)

The producer **regenerates the same deterministic catalog and latent users as
the seeder** (identical RNG seed), so the users it emits events for are exactly
the ones already in the database. Then it samples affinity-driven interactions
(`dataset.SampleForUser`) and publishes them with the *current* timestamp — a
live stream that reflects the same hidden tastes the history was built from.

Flags: `-rate` (events/sec, via a ticker) and `-count` (0 = run forever). This
stands in for real product instrumentation; in production the producer is your
app emitting real clicks.

## What we verified

Producing 300 events and running the processor populated Redis end to end:
- `trending` ZSET ranked items (scores are the large time-decayed values from
  the exponential-increment trick — expected; ranking is what matters),
- per-user `recent:*`, `catcount:*`, `price:*`, `emb:*` keys,
- `cooc:*` co-occurrence sets.

The full write path — Kafka → processor → derived Redis state + durable log —
works. Next (docs/07) the **read** path consumes this state to serve
recommendations.
