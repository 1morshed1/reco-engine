// Package config centralizes all runtime configuration.
//
// Design (see docs/01 D10): every setting comes from an environment variable
// with a default that points at the local docker-compose stack, so a fresh
// clone runs with zero configuration. Production overrides via the environment.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds every tunable the services read at startup.
type Config struct {
	PostgresDSN  string        // Postgres connection string.
	RedisAddr    string        // host:port for Redis.
	KafkaBrokers []string      // Kafka bootstrap brokers.
	KafkaTopic   string        // Topic carrying user activity events.
	KafkaGroupID string        // Consumer group for the processor.
	APIAddr      string        // Listen address for the HTTP API.
	EmbeddingDim int           // Dimensionality of item/user embeddings.
	TrendingHalfLife time.Duration // Half-life for trending time-decay.
}

// Load reads configuration from the environment, applying defaults.
func Load() Config {
	return Config{
		PostgresDSN:      env("POSTGRES_DSN", "postgres://reco:reco@localhost:5432/reco?sslmode=disable"),
		RedisAddr:        env("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:     []string{env("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopic:       env("KAFKA_TOPIC", "user-events"),
		KafkaGroupID:     env("KAFKA_GROUP_ID", "reco-processor"),
		APIAddr:          env("API_ADDR", ":8080"),
		EmbeddingDim:     envInt("EMBEDDING_DIM", 32),
		TrendingHalfLife: time.Duration(envInt("TRENDING_HALFLIFE_HOURS", 24)) * time.Hour,
	}
}

// env returns the value of key, or def if unset/empty.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt returns key parsed as an int, or def if unset/unparseable.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
