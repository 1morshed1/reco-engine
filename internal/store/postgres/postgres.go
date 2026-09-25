// Package postgres is the durable store: catalog, embeddings (pgvector),
// interaction log, and recommendation log. It is the source of truth (see
// docs/02). The online request path touches it only for ANN search and item
// hydration; the heavy history lives here for batch/eval.
//
// Note on vectors: rather than depend on a pgvector client codec, we format
// embeddings as pgvector's text form ("[a,b,c]") and cast with ::vector on the
// way in, and parse the text form on the way out. This keeps the dependency
// surface tiny and the SQL explicit.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pooled connection to Postgres and verifies it with a ping.
//
// The pool size matters for serving: the request path issues several queries per
// recommendation (ANN, item hydration, fresh), and under concurrency a too-small
// pool serializes them and inflates p99. We size it well above the default so
// concurrent requests don't queue on connections (see docs/11).
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 30
	cfg.MinConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate executes the schema SQL (idempotent, uses IF NOT EXISTS throughout).
func (s *Store) Migrate(ctx context.Context, schemaSQL string) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// itemColumns is the shared SELECT list so every item read hydrates the same
// fields. embedding::text lets us parse the vector without a codec.
const itemColumns = `id, title, category, genres, year, price, rating, created_at, COALESCE(embedding::text, '')`

// UpsertItems inserts the catalog (without embeddings). Uses a single batch for
// throughput; conflicts on id are ignored so re-seeding is safe.
func (s *Store) UpsertItems(ctx context.Context, items []models.Item) error {
	batch := &pgx.Batch{}
	for _, it := range items {
		batch.Queue(`INSERT INTO items (id, title, category, genres, year, price, rating, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (id) DO UPDATE SET
				title=EXCLUDED.title, category=EXCLUDED.category, genres=EXCLUDED.genres,
				year=EXCLUDED.year, price=EXCLUDED.price, rating=EXCLUDED.rating,
				created_at=EXCLUDED.created_at`,
			it.ID, it.Title, it.Category, it.Genres, it.Year, it.Price, it.Rating, it.CreatedAt)
	}
	return s.sendBatch(ctx, batch, len(items))
}

// SetEmbeddings writes the pgvector column for each item. Vectors are sent as
// the pgvector text literal and cast server-side.
func (s *Store) SetEmbeddings(ctx context.Context, embs map[int64][]float32) error {
	batch := &pgx.Batch{}
	for id, vec := range embs {
		batch.Queue(`UPDATE items SET embedding = $1::vector WHERE id = $2`, vecToString(vec), id)
	}
	return s.sendBatch(ctx, batch, len(embs))
}

// CreateANNIndex builds the IVFFlat cosine index. Called AFTER embeddings exist
// so the index clusters real data (see docs/03). `lists` trades build time and
// index size against recall; sqrt(rows) is the usual starting point.
func (s *Store) CreateANNIndex(ctx context.Context, rows int) error {
	lists := int(1)
	for lists*lists < rows {
		lists++
	}
	if lists < 1 {
		lists = 1
	}
	q := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_items_embedding ON items
		 USING ivfflat (embedding vector_cosine_ops) WITH (lists = %d)`, lists)
	if _, err := s.pool.Exec(ctx, q); err != nil {
		return fmt.Errorf("create ann index: %w", err)
	}
	return nil
}

// ANNSearch returns the k items nearest the query vector by cosine distance,
// with a similarity score in [0,1] (1 - cosine distance) and their embeddings
// (needed later for MMR diversity).
func (s *Store) ANNSearch(ctx context.Context, query []float32, k int) ([]models.Candidate, error) {
	q := `SELECT ` + itemColumns + `, (1 - (embedding <=> $1::vector)) AS similarity
		FROM items WHERE embedding IS NOT NULL
		ORDER BY embedding <=> $1::vector LIMIT $2`
	rows, err := s.pool.Query(ctx, q, vecToString(query), k)
	if err != nil {
		return nil, fmt.Errorf("ann search: %w", err)
	}
	defer rows.Close()

	var out []models.Candidate
	for rows.Next() {
		var it models.Item
		var embText string
		var sim float64
		if err := rows.Scan(&it.ID, &it.Title, &it.Category, &it.Genres, &it.Year,
			&it.Price, &it.Rating, &it.CreatedAt, &embText, &sim); err != nil {
			return nil, err
		}
		out = append(out, models.Candidate{
			Item:            it,
			Source:          "embedding",
			SimilarityScore: sim,
			Embedding:       parseVec(embText),
		})
	}
	return out, rows.Err()
}

// GetItemsByIDs hydrates full item records (with embeddings) for a set of IDs,
// used to turn co-occurrence/trending item IDs into ranking candidates.
func (s *Store) GetItemsByIDs(ctx context.Context, ids []int64) (map[int64]models.Candidate, error) {
	out := make(map[int64]models.Candidate, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	q := `SELECT ` + itemColumns + ` FROM items WHERE id = ANY($1)`
	rows, err := s.pool.Query(ctx, q, ids)
	if err != nil {
		return nil, fmt.Errorf("get items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var it models.Item
		var embText string
		if err := rows.Scan(&it.ID, &it.Title, &it.Category, &it.Genres, &it.Year,
			&it.Price, &it.Rating, &it.CreatedAt, &embText); err != nil {
			return nil, err
		}
		out[it.ID] = models.Candidate{Item: it, Embedding: parseVec(embText)}
	}
	return out, rows.Err()
}

// RecentItems returns freshly-created items, optionally filtered to the given
// categories. Backs the "fresh/exploration" retrieval source.
func (s *Store) RecentItems(ctx context.Context, categories []string, limit, days int) ([]models.Candidate, error) {
	q := `SELECT ` + itemColumns + `
		FROM items
		WHERE created_at > now() - make_interval(days => $1)
		  AND (cardinality($2::text[]) = 0 OR category = ANY($2))
		ORDER BY created_at DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, days, categories, limit)
	if err != nil {
		return nil, fmt.Errorf("recent items: %w", err)
	}
	defer rows.Close()
	var out []models.Candidate
	for rows.Next() {
		var it models.Item
		var embText string
		if err := rows.Scan(&it.ID, &it.Title, &it.Category, &it.Genres, &it.Year,
			&it.Price, &it.Rating, &it.CreatedAt, &embText); err != nil {
			return nil, err
		}
		out = append(out, models.Candidate{Item: it, Source: "fresh", Embedding: parseVec(embText)})
	}
	return out, rows.Err()
}

// InsertInteractions appends a batch of events to the interaction log (seeding).
func (s *Store) InsertInteractions(ctx context.Context, events []models.Event) error {
	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(`INSERT INTO interactions (user_id, item_id, event_type, value, ts)
			VALUES ($1,$2,$3,$4,$5)`, e.UserID, e.ItemID, string(e.Type), e.Value, e.Timestamp)
	}
	return s.sendBatch(ctx, batch, len(events))
}

// InsertInteraction appends a single event (the processor's live write path).
func (s *Store) InsertInteraction(ctx context.Context, e models.Event) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO interactions (user_id, item_id, event_type, value, ts)
		VALUES ($1,$2,$3,$4,$5)`, e.UserID, e.ItemID, string(e.Type), e.Value, e.Timestamp)
	return err
}

// LogRecommendations records a served list for later CTR/online evaluation.
func (s *Store) LogRecommendations(ctx context.Context, userID int64, items []models.ScoredItem) error {
	batch := &pgx.Batch{}
	for i, it := range items {
		batch.Queue(`INSERT INTO recommendations (user_id, item_id, rank, source, score)
			VALUES ($1,$2,$3,$4,$5)`, userID, it.Item.ID, i+1, it.Source, it.Score)
	}
	return s.sendBatch(ctx, batch, len(items))
}

// MarkClicked flags the most recent served recommendation of (user,item) as
// clicked, closing the loop for CTR/online evaluation.
func (s *Store) MarkClicked(ctx context.Context, userID, itemID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE recommendations SET clicked = true
		WHERE id = (
			SELECT id FROM recommendations
			WHERE user_id = $1 AND item_id = $2
			ORDER BY served_at DESC LIMIT 1
		)`, userID, itemID)
	return err
}

// UserInteractions returns all item IDs a user interacted with (for eval ground
// truth and for filtering already-seen items).
func (s *Store) UserInteractions(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT item_id FROM interactions WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// AllUserIDs lists every user present in the interaction log (for eval sweeps).
func (s *Store) AllUserIDs(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT user_id FROM interactions ORDER BY user_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// AllInteractions streams the full interaction log in chronological order. Used
// by the offline evaluator to build a leakage-free train/test split.
func (s *Store) AllInteractions(ctx context.Context) ([]models.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id, item_id, event_type, value, ts FROM interactions ORDER BY ts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.Event
	for rows.Next() {
		var e models.Event
		var t string
		if err := rows.Scan(&e.UserID, &e.ItemID, &t, &e.Value, &e.Timestamp); err != nil {
			return nil, err
		}
		e.Type = models.EventType(t)
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountItems returns the number of catalog rows (used to size the ANN index).
func (s *Store) CountItems(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&n)
	return n, err
}

// AllItems streams the whole catalog (with embeddings) — used by the processor
// to compute user embeddings and by eval for coverage.
func (s *Store) AllItems(ctx context.Context) ([]models.Item, map[int64][]float32, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+itemColumns+` FROM items`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var items []models.Item
	embs := map[int64][]float32{}
	for rows.Next() {
		var it models.Item
		var embText string
		if err := rows.Scan(&it.ID, &it.Title, &it.Category, &it.Genres, &it.Year,
			&it.Price, &it.Rating, &it.CreatedAt, &embText); err != nil {
			return nil, nil, err
		}
		items = append(items, it)
		if embText != "" {
			embs[it.ID] = parseVec(embText)
		}
	}
	return items, embs, rows.Err()
}

// sendBatch runs a batch and drains every result so errors surface.
func (s *Store) sendBatch(ctx context.Context, batch *pgx.Batch, n int) error {
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < n; i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec: %w", err)
		}
	}
	return nil
}

// --- vector text encoding (pgvector literal form) ---

// vecToString renders a vector as pgvector's text literal: "[0.1,0.2,...]".
func vecToString(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// parseVec parses pgvector's text form back into a float slice. Returns nil on
// empty/malformed input.
func parseVec(s string) []float32 {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' {
		return nil
	}
	s = s[1 : len(s)-1]
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil
		}
		out = append(out, float32(f))
	}
	return out
}
