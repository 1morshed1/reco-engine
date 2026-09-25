# 09 — Evaluation: Measuring Whether It's Any Good

This doc covers `cmd/eval`. Building a recommender is easy; knowing if it *works*
is the hard, respected part. This is what separates "I made a thing" from "I made
a thing and here's proof it beats the baseline."

## The golden rule: no leakage

The single most important idea in offline evaluation: **the recommender must not
see the answers.** If any signal used to make a recommendation was derived from
the items you're testing against, your metrics are fiction (they'll look amazing
and mean nothing).

So the evaluator does NOT reuse the live pipeline — the live pipeline reads Redis
state built from *all* events, which includes the test items. Instead it rebuilds
everything from a clean split:

- **Temporal split per user:** sort a user's interactions by time, take the
  earliest **80% as train** (allowed history) and the latest **20% as test**
  (the items to predict). Temporal — not random — because in production you always
  predict the future from the past.
- **Every signal is train-only:** the user's taste vector is the average of
  *train* item embeddings; preferred categories come from *train*; global
  popularity is counted from *train* portions across all users.
- **Recommendations exclude train items** — you don't recommend what someone
  already engaged with.

## The three strategies (an A/B simulation)

We compare, on identical data:

1. **popularity** — recommend the globally most-interacted items. No
   personalization. The baseline every recommender must beat.
2. **embedding** — the K items whose vectors are nearest the user's taste vector
   (pure content/ANN).
3. **full** — a blended score: `1.0·similarity + 0.3·log1p(normPopularity) +
   0.6·categoryMatch + 0.3·rating`. A stand-in for the production ranker.

## The metrics

For each user, with their held-out test set as ground truth:

- **precision@K** = (# recommended that were in test) / K. "Of what I showed, how
  much was right?"
- **recall@K** = (# recommended that were in test) / |test|. "Of what they'd
  engage with, how much did I surface?"
- **nDCG@K** — ranking quality. A hit at rank 1 is worth more than a hit at rank
  10 (`gain = 1/log2(rank+1)`), normalized by the best possible ordering. This is
  the metric that cares about *order*, not just membership.
- **coverage** = fraction of the whole catalog that ever gets recommended across
  all users. **Low coverage = filter bubble** — the system shows everyone the
  same few items.

## The results (and how to read them)

```
strategy      precision   recall      nDCG        coverage
popularity    0.0056      0.0077      0.0066      0.0070
embedding     0.0074      0.0103      0.0082      0.1885
full          0.0072      0.0097      0.0092      0.2135
```

What this tells us:
- **Personalization beats the baseline.** Both `embedding` and `full` clearly
  outrank `popularity` on precision, recall, and nDCG. That's the headline result
  — the personalized pipeline is doing real work.
- **`full` has the best ranking quality (nDCG)** — the category and rating signals
  push the *right* items higher, which is exactly what nDCG rewards, even though
  its raw precision is a hair below pure embedding.
- **Coverage is the dramatic one.** Popularity recommends ~0.7% of the catalog
  (≈14 items to everyone — a textbook filter bubble). The personalized strategies
  cover 19–21% — an order of magnitude more of the catalog reaches users. `full`
  is the most diverse.

### Why are the absolute numbers small?

They're *supposed* to be. Each user's test set is ~20% of ~44 interactions ≈ 9
items, and we're picking 10 out of a 2000-item catalog. Hitting even a few is
hard, and offline metrics on sparse implicit feedback are always low in absolute
terms. **What matters is the relative comparison** — and the ordering
(personalized ≫ popularity, full best on nDCG/coverage) is exactly what a working
system should show.

## Offline vs online (the honest limitation)

Offline metrics measure "would the user have interacted with this, historically."
They can't measure whether a *new* recommendation the user never saw is good — the
held-out set only contains items they happened to encounter. That's why real
systems also run **online** A/B tests measuring live **CTR** and **conversion**.

We built the hook for that: every served list is logged (`recommendations`
table), and `POST /api/v1/click` flips `clicked = true`. Computing live CTR is
then a `SELECT avg(clicked::int)` over served rows — the online counterpart to
this offline harness. In production the batch layer (docs/10) would run this
offline eval nightly and the online CTR would come off live traffic.
