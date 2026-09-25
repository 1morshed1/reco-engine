// Package metrics defines the Prometheus instruments the API exposes on
// /metrics. Keeping them in one place makes the observable surface explicit (see
// docs/08). These are the four numbers the plan calls for: recommendation
// latency, candidate pool size, request counts, and cache-hit rate.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Latency is the end-to-end recommendation serving time. Buckets are tuned
	// around the sub-50ms target so p99 is readable on a dashboard.
	Latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "recommendation_latency_seconds",
		Help:    "End-to-end recommendation request latency.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})

	// Requests counts recommendation requests by outcome.
	Requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recommendation_requests_total",
		Help: "Recommendation requests, labeled by status (ok|error).",
	}, []string{"status"})

	// CandidatePoolSize tracks how many candidates retrieval produced — a proxy
	// for recall and a debugging aid (a shrinking pool means a starved source).
	CandidatePoolSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "recommendation_candidate_pool_size",
		Help:    "Number of candidates produced by Stage 1 retrieval.",
		Buckets: []float64{0, 10, 25, 50, 100, 150, 200, 300, 500},
	})

	// ProfileHit / ProfileMiss approximate the cache-hit rate: a "hit" is a user
	// who already has derived state (a non-empty profile), a "miss" is a cold
	// user served purely from trending/fresh.
	ProfileHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "recommendation_profile_hits_total",
		Help: "Requests where the user had a warm profile (interaction history).",
	})
	ProfileMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "recommendation_profile_misses_total",
		Help: "Requests for cold users with no profile yet.",
	})
)

// Handler returns the /metrics HTTP handler for Prometheus to scrape.
func Handler() http.Handler { return promhttp.Handler() }
