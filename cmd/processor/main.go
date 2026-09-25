// Command processor is the streaming write path. It consumes user activity
// events from Kafka and applies the derived-state updates (Redis profiles,
// co-occurrence, trending) plus the durable interaction-log append.
//
// Run it alongside the producer and API:
//
//	go run ./cmd/processor
//
// It replays existing topic history on first run (consumer group starts at the
// earliest offset), so a fresh stack warms Redis from whatever's already in
// Kafka.
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/events"
	"github.com/1morshed1/reco-engine/internal/profile"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
)

func main() {
	cfg := config.Load()

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pg, err := postgres.New(initCtx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()

	rs, err := redisstore.New(initCtx, cfg.RedisAddr, cfg.TrendingHalfLife)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rs.Close()

	updater, err := profile.New(initCtx, rs, pg, profile.Config{
		EmbeddingDim: cfg.EmbeddingDim,
		RecentItems:  20,
	})
	if err != nil {
		log.Fatalf("profile updater: %v", err)
	}

	consumer := events.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	log.Printf("processor started: topic=%q group=%q", cfg.KafkaTopic, cfg.KafkaGroupID)

	var processed int64
	logEvery := int64(500)
	for {
		ev, err := consumer.Read(ctx)
		if err != nil {
			// Context cancelled (shutdown) or reader closed -> exit cleanly.
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				log.Printf("shutting down; processed %d events", processed)
				return
			}
			log.Printf("read error: %v", err)
			continue
		}

		// Give each event a bounded processing budget independent of shutdown so
		// an in-flight update can finish, but not hang forever.
		hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := updater.Handle(hctx, ev); err != nil {
			log.Printf("handle event (user=%d item=%d): %v", ev.UserID, ev.ItemID, err)
		}
		hcancel()

		processed++
		if processed%logEvery == 0 {
			log.Printf("processed %d events", processed)
		}
	}
}
