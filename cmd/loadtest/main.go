// Command loadtest hammers the recommendation API concurrently and reports
// throughput and latency percentiles — the serving benchmarks the plan calls for
// (docs/11). It's a tiny, dependency-free load generator so the whole benchmark
// runs with just the Go toolchain.
//
//	go run ./cmd/loadtest -c 50 -d 10s -users 500
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "API base URL")
	conc := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 10*time.Second, "test duration")
	users := flag.Int("users", 500, "user ID space to sample from")
	n := flag.Int("n", 20, "recommendations per request")
	flag.Parse()

	// Shared HTTP client tuned for many concurrent keep-alive connections.
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *conc * 2,
			MaxIdleConnsPerHost: *conc * 2,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		latency  []time.Duration
		requests int64
		errors   int64
	)

	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var local []time.Duration
			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					latency = append(latency, local...)
					mu.Unlock()
					return
				default:
				}
				uid := 1 + rng.Intn(*users)
				url := fmt.Sprintf("%s/api/v1/recommend/%d?n=%d", *addr, uid, *n)
				t0 := time.Now()
				resp, err := client.Get(url)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					atomic.AddInt64(&errors, 1)
					continue
				}
				local = append(local, time.Since(t0))
				atomic.AddInt64(&requests, 1)
			}
		}(int64(w) + 1)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if len(latency) == 0 {
		fmt.Fprintln(os.Stderr, "no successful requests — is the API up?")
		os.Exit(1)
	}
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })

	qps := float64(requests) / elapsed.Seconds()
	fmt.Printf("\n=== Load test (%d workers, %s) ===\n", *conc, elapsed.Round(time.Millisecond))
	fmt.Printf("requests:   %d (%d errors)\n", requests, errors)
	fmt.Printf("throughput: %.0f req/s\n", qps)
	fmt.Printf("latency p50: %s\n", pct(latency, 0.50))
	fmt.Printf("latency p90: %s\n", pct(latency, 0.90))
	fmt.Printf("latency p99: %s\n", pct(latency, 0.99))
	fmt.Printf("latency max: %s\n", latency[len(latency)-1].Round(time.Microsecond))
	fmt.Println()
}

// pct returns the p-quantile of a sorted duration slice.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Round(time.Microsecond)
}
