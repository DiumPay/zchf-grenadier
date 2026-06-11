package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	pricesURL    = "https://api.frankencoin.com/prices/list"
	pricesTTL    = 60 * time.Second
	pricesHTTPTO = 10 * time.Second
)

// TrackedSymbols are the illiquid ones where defillama coverage is thin or
// the price is protocol-derived (FPS). Anything else just returns nil — we
// don't want to be a generic price proxy.
var TrackedSymbols = map[string]bool{
	"BOSS":  true,
	"REALU": true,
	"SPYON": true, // case-insensitive lookup, stored uppercase
	"LENDS": true,
	"FPS":   true,
	"DQTS":  true,
}

// Price is a row from upstream /prices/list, slimmed.
type Price struct {
	Address   string  `json:"address"`
	Name      string  `json:"name"`
	Symbol    string  `json:"symbol"`
	Decimals  int     `json:"decimals"`
	ChainID   int     `json:"chainId"`
	Source    string  `json:"source"`
	Timestamp int64   `json:"timestamp"`
	CHF       float64 `json:"chf"`
	USD       float64 `json:"usd"`
}

// upstreamRow matches the actual /prices/list response so we can decode it.
type upstreamRow struct {
	ChainID   int    `json:"chainId"`
	Address   string `json:"address"`
	Name      string `json:"name"`
	Symbol    string `json:"symbol"`
	Decimals  int    `json:"decimals"`
	Source    string `json:"source"`
	Timestamp int64  `json:"timestamp"`
	Price     struct {
		CHF float64 `json:"chf"`
		USD float64 `json:"usd"`
	} `json:"price"`
}

// PriceCache: lazy, in-memory, TTL'd. Goroutine-safe.
//
// On a request:
//   - if cache fresh → serve from memory, zero network
//   - if cache stale → one upstream call, refresh all tracked symbols, serve
//   - if upstream fails and we have stale data → serve stale (better than nothing)
//   - if upstream fails and we have no data → return nil, caller decides
//
// Single-flight: concurrent requests during a refresh share the same upstream
// call so we never hammer api.frankencoin.com.
type PriceCache struct {
	mu       sync.RWMutex
	bySymbol map[string]*Price // key: uppercase symbol
	lastFill time.Time
	inflight chan struct{} // nil when idle, closed-when-done channel during fetch
	httpc    *http.Client
}

func NewPriceCache() *PriceCache {
	return &PriceCache{
		bySymbol: make(map[string]*Price, len(TrackedSymbols)),
		httpc:    &http.Client{Timeout: pricesHTTPTO},
	}
}

// Get returns the cached price for a symbol, refreshing from upstream if stale.
// Returns nil if the symbol isn't tracked or upstream is unreachable with no
// prior cache.
func (pc *PriceCache) Get(ctx context.Context, symbol string) *Price {
	sym := strings.ToUpper(symbol)
	if !TrackedSymbols[sym] {
		return nil
	}

	// Fast path: fresh cache, no lock contention beyond a read lock.
	pc.mu.RLock()
	p, ok := pc.bySymbol[sym]
	fresh := time.Since(pc.lastFill) < pricesTTL
	pc.mu.RUnlock()

	if ok && fresh {
		return p
	}

	// Slow path: trigger a refresh (single-flighted), then read again.
	pc.refresh(ctx)

	pc.mu.RLock()
	p = pc.bySymbol[sym]
	pc.mu.RUnlock()
	return p
}

// All returns a snapshot of every tracked symbol. Triggers a refresh if stale.
func (pc *PriceCache) All(ctx context.Context) map[string]*Price {
	pc.mu.RLock()
	fresh := time.Since(pc.lastFill) < pricesTTL
	pc.mu.RUnlock()

	if !fresh {
		pc.refresh(ctx)
	}

	pc.mu.RLock()
	defer pc.mu.RUnlock()
	out := make(map[string]*Price, len(pc.bySymbol))
	for k, v := range pc.bySymbol {
		out[k] = v
	}
	return out
}

// refresh: at most one outbound HTTP call at a time. Other callers wait on
// the same channel and read the result from cache.
func (pc *PriceCache) refresh(ctx context.Context) {
	pc.mu.Lock()
	if pc.inflight != nil {
		// Another goroutine is fetching — wait for it.
		ch := pc.inflight
		pc.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		return
	}
	ch := make(chan struct{})
	pc.inflight = ch
	pc.mu.Unlock()

	defer func() {
		pc.mu.Lock()
		pc.inflight = nil
		close(ch)
		pc.mu.Unlock()
	}()

	rows, err := pc.fetch(ctx)
	if err != nil {
		fmt.Printf("[prices] upstream fetch: %v (serving stale)\n", err)
		// Stamp lastFill even on failure so subsequent /prices requests
		// honor the TTL window instead of each one re-triggering a fresh
		// upstream attempt. Without this, every API call while upstream
		// is down spawns a 10s-timing-out HTTP request — turning one
		// dead dependency into a self-inflicted DoS amplifier.
		pc.mu.Lock()
		pc.lastFill = time.Now()
		pc.mu.Unlock()
		return
	}

	next := make(map[string]*Price, len(TrackedSymbols))
	for _, r := range rows {
		sym := strings.ToUpper(r.Symbol)
		if !TrackedSymbols[sym] {
			continue
		}
		next[sym] = &Price{
			Address:   strings.ToLower(r.Address),
			Name:      r.Name,
			Symbol:    r.Symbol,
			Decimals:  r.Decimals,
			ChainID:   r.ChainID,
			Source:    r.Source,
			Timestamp: r.Timestamp,
			CHF:       r.Price.CHF,
			USD:       r.Price.USD,
		}
	}

	pc.mu.Lock()
	pc.bySymbol = next
	pc.lastFill = time.Now()
	pc.mu.Unlock()
}

func (pc *PriceCache) fetch(ctx context.Context) ([]upstreamRow, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", pricesURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := pc.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var rows []upstreamRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}
