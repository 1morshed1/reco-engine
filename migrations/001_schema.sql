-- Schema for the recommendation engine.
--
-- Three concerns live here (see docs/02):
--   1. The catalog (items) — the durable source of truth.
--   2. The interaction log — every user event, appended forever. This is what
--      the batch layer re-trains embeddings and co-occurrence from, and what the
--      offline evaluator measures against.
--   3. The recommendation log — what we served and what got clicked, for CTR /
--      online evaluation.
--
-- The embedding lives on the item row as a pgvector column so ANN search and a
-- metadata join happen in one query (see docs/01 D4).

-- pgvector provides the `vector` type and ANN index operators.
CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------------
-- Catalog
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS items (
    id          BIGINT PRIMARY KEY,
    title       TEXT        NOT NULL,
    category    TEXT        NOT NULL,           -- primary genre
    genres      TEXT[]      NOT NULL DEFAULT '{}',
    year        INT         NOT NULL DEFAULT 0,
    price       DOUBLE PRECISION NOT NULL DEFAULT 0,
    rating      DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Embedding dimensionality must match config.EmbeddingDim (default 32).
    -- Nullable because seed inserts rows first, then fills embeddings.
    embedding   vector(32)
);

-- Filter candidates by category quickly (used by trending/fresh retrieval).
CREATE INDEX IF NOT EXISTS idx_items_category ON items (category);
CREATE INDEX IF NOT EXISTS idx_items_created_at ON items (created_at DESC);

-- ANN index for embedding similarity search. IVFFlat needs the table populated
-- before it can build well, so seed creates it AFTER inserting embeddings.
-- (Left out of the base schema on purpose; see cmd/seed.)

-- ---------------------------------------------------------------------------
-- Interaction log (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS interactions (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT      NOT NULL,
    item_id    BIGINT      NOT NULL,
    event_type TEXT        NOT NULL,
    value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions (user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_interactions_item ON interactions (item_id);

-- ---------------------------------------------------------------------------
-- Recommendation log (what we served; for CTR / online eval)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recommendations (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT      NOT NULL,
    item_id    BIGINT      NOT NULL,
    rank       INT         NOT NULL,        -- position in the served list (1 = top)
    source     TEXT        NOT NULL,        -- retrieval source that produced it
    score      DOUBLE PRECISION NOT NULL,
    served_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    clicked    BOOLEAN     NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_recs_user ON recommendations (user_id, served_at DESC);
