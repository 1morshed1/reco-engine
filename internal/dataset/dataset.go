// Package dataset generates a self-contained, deterministic synthetic catalog
// and user activity. See docs/01 D5 and docs/04 for the rationale.
//
// Why synthetic instead of downloading MovieLens: it makes the whole project
// run offline with zero downloads, and — crucially — because WE define the
// latent user preferences, the offline evaluator has ground truth to measure
// against. Real behavior is generated from a hidden "taste" model; a good
// recommender should recover that taste. Everything is seeded, so two runs
// produce identical data (reproducible experiments).
package dataset

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/1morshed1/reco-engine/internal/embedding"
	"github.com/1morshed1/reco-engine/internal/models"
)

// Params control the size of the generated world.
type Params struct {
	NumItems        int   // catalog size
	NumUsers        int   // number of simulated users
	EventsPerUser   int   // historical interactions generated per user
	Seed            int64 // RNG seed for reproducibility
	HistoryDays     int   // spread events over the last N days
}

// Default returns sensible sizes for a laptop-scale demo.
func Default() Params {
	return Params{NumItems: 2000, NumUsers: 500, EventsPerUser: 40, Seed: 42, HistoryDays: 60}
}

// titleWords seed procedural movie titles; purely cosmetic.
var titleWords = []string{
	"Shadow", "Empire", "Midnight", "Crimson", "Silent", "Broken", "Last",
	"Golden", "Frozen", "Rising", "Hidden", "Eternal", "Savage", "Distant",
	"Velvet", "Iron", "Neon", "Lost", "Sacred", "Wild", "Electric", "Hollow",
}
var titleNouns = []string{
	"Horizon", "Kingdom", "Protocol", "Legacy", "Requiem", "Voyage", "Paradox",
	"Sanctuary", "Odyssey", "Covenant", "Mirage", "Dynasty", "Eclipse", "Frontier",
	"Genesis", "Labyrinth", "Serpent", "Tempest", "Vanguard", "Zenith",
}

// Catalog builds a deterministic list of items. Each item gets 1–3 genres, a
// year, a rating, and a price loosely correlated with rating and recency (so
// price-match ranking has something real to latch onto).
func Catalog(p Params) []models.Item {
	rng := rand.New(rand.NewSource(p.Seed))
	now := time.Now()
	items := make([]models.Item, 0, p.NumItems)

	for i := 1; i <= p.NumItems; i++ {
		nGenres := 1 + rng.Intn(3)
		genres := pickGenres(rng, nGenres)
		year := 1980 + rng.Intn(45) // 1980..2024
		// Rating ~ N(3.5, 0.7), clamped to [1,5].
		rating := clamp(3.5+rng.NormFloat64()*0.7, 1, 5)
		// Price: newer + higher-rated => pricier, plus noise. Range ~ [5, 40].
		price := 5 + (float64(year-1980)/45)*20 + (rating/5)*10 + rng.Float64()*5

		// Spread created_at across the last HistoryDays*2 so "fresh" retrieval
		// has recent items and recency ranking has a gradient.
		created := now.Add(-time.Duration(rng.Intn(p.HistoryDays*2*24)) * time.Hour)

		title := fmt.Sprintf("%s %s", titleWords[rng.Intn(len(titleWords))], titleNouns[rng.Intn(len(titleNouns))])
		items = append(items, models.Item{
			ID:        int64(i),
			Title:     title,
			Category:  genres[0],
			Genres:    genres,
			Year:      year,
			Price:     round2(price),
			Rating:    round2(rating),
			CreatedAt: created,
		})
	}
	return items
}

// LatentUser is the hidden "true taste" of a simulated user. The recommender
// never sees this — it only sees the events it produces. The evaluator uses it
// (indirectly, via held-out interactions) as ground truth.
type LatentUser struct {
	ID          int64
	FavGenres   []string // genres this user actually likes
	PriceMean   float64  // preferred price point
	PriceStd    float64
}

// Users builds deterministic latent users, each with 1–3 favorite genres and a
// price preference.
func Users(p Params) []LatentUser {
	rng := rand.New(rand.NewSource(p.Seed + 1))
	users := make([]LatentUser, 0, p.NumUsers)
	for i := 1; i <= p.NumUsers; i++ {
		fav := pickGenres(rng, 1+rng.Intn(3))
		users = append(users, LatentUser{
			ID:        int64(i),
			FavGenres: fav,
			PriceMean: 10 + rng.Float64()*25, // 10..35
			PriceStd:  3 + rng.Float64()*5,
		})
	}
	return users
}

// affinity scores how much a latent user "likes" an item: genre overlap is the
// main driver, with a mild price-fit bonus and the item's own quality (rating).
func (u LatentUser) affinity(item models.Item) float64 {
	overlap := 0.0
	for _, g := range item.Genres {
		if contains(u.FavGenres, g) {
			overlap++
		}
	}
	genreScore := overlap / float64(len(u.FavGenres)) // 0..1+
	priceFit := math.Exp(-0.5 * math.Pow((item.Price-u.PriceMean)/u.PriceStd, 2))
	return 2.0*genreScore + 0.5*priceFit + 0.3*(item.Rating/5)
}

// GenerateInteractions simulates each user's historical activity. For every user
// we sample items with probability proportional to affinity (softmax-ish), then
// emit a plausible event (mostly views, some clicks/purchases/ratings). Events
// are timestamped randomly within the history window.
//
// The returned events are ordered by timestamp so replaying them reproduces a
// realistic arrival order.
func GenerateInteractions(p Params, catalog []models.Item, users []LatentUser) []models.Event {
	rng := rand.New(rand.NewSource(p.Seed + 2))
	now := time.Now()
	var events []models.Event

	// Precompute per-user a weighted item sampler over a candidate subset for
	// speed (scoring all items per draw would be O(users*events*items)).
	for _, u := range users {
		// Build a shortlist: sample 200 random items, keep affinity weights.
		shortlist := make([]models.Item, 0, 200)
		weights := make([]float64, 0, 200)
		var total float64
		for k := 0; k < 200; k++ {
			it := catalog[rng.Intn(len(catalog))]
			w := math.Exp(u.affinity(it)) // softmax weight
			shortlist = append(shortlist, it)
			weights = append(weights, w)
			total += w
		}
		for e := 0; e < p.EventsPerUser; e++ {
			it := sampleWeighted(rng, shortlist, weights, total)
			ts := now.Add(-time.Duration(rng.Intn(p.HistoryDays*24)) * time.Hour)
			ev := models.Event{UserID: u.ID, ItemID: it.ID, Timestamp: ts}
			// Event type distribution: 60% view, 20% click, 12% rating, 8% purchase.
			switch r := rng.Float64(); {
			case r < 0.60:
				ev.Type = models.EventView
			case r < 0.80:
				ev.Type = models.EventClick
			case r < 0.92:
				ev.Type = models.EventRating
				// Ratings reflect affinity: liked items rated higher.
				ev.Value = clamp(math.Round(2+u.affinity(it)), 1, 5)
			default:
				ev.Type = models.EventPurchase
			}
			events = append(events, ev)
		}
	}
	// Sort by timestamp so downstream replay sees chronological order.
	sortByTime(events)
	return events
}

// SampleForUser returns one affinity-weighted random item for the user, drawn
// from a fresh random shortlist. Exported so the live producer can generate
// realistic events using the same taste model as the seeder.
func SampleForUser(rng *rand.Rand, u LatentUser, catalog []models.Item, shortlist int) models.Item {
	items := make([]models.Item, 0, shortlist)
	weights := make([]float64, 0, shortlist)
	var total float64
	for k := 0; k < shortlist; k++ {
		it := catalog[rng.Intn(len(catalog))]
		w := math.Exp(u.affinity(it))
		items = append(items, it)
		weights = append(weights, w)
		total += w
	}
	return sampleWeighted(rng, items, weights, total)
}

// Embed computes and returns content embeddings for every item, keyed by item
// ID. Used by the seeder to populate the pgvector column.
func Embed(catalog []models.Item, dim int) map[int64][]float32 {
	out := make(map[int64][]float32, len(catalog))
	for _, it := range catalog {
		out[it.ID] = embedding.ItemVector(it, dim)
	}
	return out
}

// --- helpers ---

func pickGenres(rng *rand.Rand, n int) []string {
	perm := rng.Perm(len(embedding.Genres))
	out := make([]string, 0, n)
	for i := 0; i < n && i < len(perm); i++ {
		out = append(out, embedding.Genres[perm[i]])
	}
	return out
}

func sampleWeighted(rng *rand.Rand, items []models.Item, weights []float64, total float64) models.Item {
	target := rng.Float64() * total
	var cum float64
	for i, w := range weights {
		cum += w
		if cum >= target {
			return items[i]
		}
	}
	return items[len(items)-1]
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }

func sortByTime(events []models.Event) {
	sort.Slice(events, func(i, j int) bool { return events[i].Timestamp.Before(events[j].Timestamp) })
}
