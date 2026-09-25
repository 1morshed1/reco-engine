// Command producer simulates live user activity and publishes it to Kafka.
//
// It regenerates the SAME deterministic catalog and latent users as the seeder
// (identical seed), so the users it emits events for match the users/items
// already in the database. It then continuously samples affinity-driven
// interactions and publishes them with the current timestamp — a realistic
// event stream for the processor to consume.
//
//	go run ./cmd/producer                 # 50 events/sec, 2000 events
//	go run ./cmd/producer -rate 200 -count 0   # 200/sec, run forever
package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/dataset"
	"github.com/1morshed1/reco-engine/internal/events"
	"github.com/1morshed1/reco-engine/internal/models"
)

func main() {
	rate := flag.Int("rate", 50, "events per second")
	count := flag.Int("count", 2000, "number of events to emit (0 = run forever)")
	flag.Parse()

	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Rebuild the same world the seeder used, so user/item IDs line up.
	p := dataset.Default()
	catalog := dataset.Catalog(p)
	users := dataset.Users(p)
	streamer := newStreamer(catalog, users)

	prod := events.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer prod.Close()

	interval := time.Second / time.Duration(max(1, *rate))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("producer started: rate=%d/s count=%d topic=%q", *rate, *count, cfg.KafkaTopic)

	var emitted int
	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped; emitted %d events", emitted)
			return
		case <-ticker.C:
			ev := streamer.next()
			if err := prod.Publish(ctx, ev); err != nil {
				log.Printf("publish error: %v", err)
				continue
			}
			emitted++
			if emitted%500 == 0 {
				log.Printf("emitted %d events", emitted)
			}
			if *count > 0 && emitted >= *count {
				log.Printf("done; emitted %d events", emitted)
				return
			}
		}
	}
}

// streamer samples realistic events on demand using the latent taste model.
type streamer struct {
	rng     *rand.Rand
	catalog []models.Item
	users   []dataset.LatentUser
}

func newStreamer(catalog []models.Item, users []dataset.LatentUser) *streamer {
	return &streamer{
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		catalog: catalog,
		users:   users,
	}
}

// next picks a random user and an affinity-weighted item, returning a fresh
// timestamped event. It reuses the dataset's public sampler for consistency.
func (s *streamer) next() models.Event {
	u := s.users[s.rng.Intn(len(s.users))]
	item := dataset.SampleForUser(s.rng, u, s.catalog, 100)
	ev := models.Event{UserID: u.ID, ItemID: item.ID, Timestamp: time.Now()}
	switch r := s.rng.Float64(); {
	case r < 0.60:
		ev.Type = models.EventView
	case r < 0.80:
		ev.Type = models.EventClick
	case r < 0.92:
		ev.Type = models.EventRating
		ev.Value = float64(2 + s.rng.Intn(4)) // 2..5
	default:
		ev.Type = models.EventPurchase
	}
	return ev
}
