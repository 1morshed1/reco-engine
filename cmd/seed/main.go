// Command seed bootstraps the database: it applies the schema, generates the
// synthetic catalog + interaction history, computes content embeddings, and
// builds the ANN index. Idempotent — safe to run repeatedly.
//
// Order matters (see docs/03): items and embeddings must exist BEFORE the
// IVFFlat index is created, or the index clusters nothing and recall suffers.
//
//	go run ./cmd/seed              # full seed
//	go run ./cmd/seed -migrate-only # just create the schema
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/dataset"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/migrations"
)

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "apply schema and exit")
	flag.Parse()

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	log.Println("applying schema...")
	if err := db.Migrate(ctx, migrations.Schema); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if *migrateOnly {
		log.Println("schema applied; exiting (migrate-only).")
		return
	}

	p := dataset.Default()
	log.Printf("generating world: %d items, %d users, %d events/user...",
		p.NumItems, p.NumUsers, p.EventsPerUser)
	catalog := dataset.Catalog(p)
	users := dataset.Users(p)
	events := dataset.GenerateInteractions(p, catalog, users)
	embs := dataset.Embed(catalog, cfg.EmbeddingDim)

	log.Printf("inserting %d items...", len(catalog))
	if err := db.UpsertItems(ctx, catalog); err != nil {
		log.Fatalf("upsert items: %v", err)
	}
	log.Printf("writing %d embeddings...", len(embs))
	if err := db.SetEmbeddings(ctx, embs); err != nil {
		log.Fatalf("set embeddings: %v", err)
	}
	log.Printf("inserting %d interactions...", len(events))
	if err := db.InsertInteractions(ctx, events); err != nil {
		log.Fatalf("insert interactions: %v", err)
	}

	log.Println("building ANN index (after data load)...")
	if err := db.CreateANNIndex(ctx, len(catalog)); err != nil {
		log.Fatalf("ann index: %v", err)
	}

	log.Printf("seed complete: %d items, %d users, %d events.",
		len(catalog), len(users), len(events))
}
