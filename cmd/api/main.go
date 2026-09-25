// Command api serves recommendations over HTTP. It wires the stores and the
// pipeline together and exposes:
//
//	GET  /api/v1/recommend/:user_id?n=20   -> ranked, diverse recommendations
//	POST /api/v1/click                     -> record a click (for CTR)
//	GET  /healthz                          -> liveness
//	GET  /metrics                          -> Prometheus metrics
//
//	go run ./cmd/api
package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/1morshed1/reco-engine/internal/candidates"
	"github.com/1morshed1/reco-engine/internal/catalog"
	"github.com/1morshed1/reco-engine/internal/config"
	"github.com/1morshed1/reco-engine/internal/metrics"
	"github.com/1morshed1/reco-engine/internal/ranking"
	"github.com/1morshed1/reco-engine/internal/recommender"
	"github.com/1morshed1/reco-engine/internal/store/postgres"
	"github.com/1morshed1/reco-engine/internal/store/redisstore"
)

func main() {
	cfg := config.Load()

	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// Load the catalog into memory once so the request path stays off Postgres
	// for item hydration and fresh listing (only ANN still queries the DB).
	cat, err := catalog.Load(initCtx, pg)
	if err != nil {
		log.Fatalf("catalog: %v", err)
	}
	log.Printf("catalog cached in memory: %d items", cat.Size())

	retriever := candidates.New(rs, pg, cat, candidates.DefaultConfig())
	ranker := ranking.New(ranking.DefaultWeights())
	svc := recommender.New(rs, pg, retriever, ranker, recommender.DefaultConfig())

	srv := &server{svc: svc, pg: pg}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/metrics", gin.WrapH(metrics.Handler()))

	v1 := r.Group("/api/v1")
	v1.GET("/recommend/:user_id", srv.recommend)
	v1.POST("/click", srv.click)

	log.Printf("API listening on %s", cfg.APIAddr)
	if err := r.Run(cfg.APIAddr); err != nil {
		log.Fatalf("server: %v", err)
	}
}

type server struct {
	svc *recommender.Service
	pg  *postgres.Store
}

// recommend handles GET /api/v1/recommend/:user_id?n=20.
func (s *server) recommend(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("user_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
		return
	}
	n := clampN(c.Query("n"), 20, 100)

	// Per-request timeout so a slow dependency can't hang a client forever.
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	res, err := s.svc.Recommend(ctx, userID, n)
	if err != nil {
		metrics.Requests.WithLabelValues("error").Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Record metrics.
	metrics.Requests.WithLabelValues("ok").Inc()
	metrics.Latency.Observe(res.TookMS / 1000.0)
	metrics.CandidatePoolSize.Observe(float64(res.CandidatePoolSize))
	if res.CandidatePoolSize > 0 && len(res.Items) > 0 {
		metrics.ProfileHits.Inc()
	} else if len(res.Items) == 0 {
		metrics.ProfileMisses.Inc()
	}

	c.JSON(http.StatusOK, res)
}

// clickRequest is the body of POST /api/v1/click.
type clickRequest struct {
	UserID int64 `json:"user_id"`
	ItemID int64 `json:"item_id"`
}

// click records that a user clicked a recommended item (updates CTR).
func (s *server) click(c *gin.Context) {
	var req clickRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.UserID == 0 || req.ItemID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id and item_id required"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := s.pg.MarkClicked(ctx, req.UserID, req.ItemID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "recorded"})
}

// clampN parses the n query param, defaulting and capping it.
func clampN(raw string, def, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}
