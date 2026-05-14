package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Pool []string

	PerAttemptTimeout  time.Duration
	TotalBudget        time.Duration
	MaxRetries         int
	ColdFanout         int
	WarmFanout         int
	PromoteWins        int
	StickyTTL          time.Duration
	CallTTL            time.Duration
	MaxCacheEntries    int
	EWMAAlpha          float64
	CooldownDuration   time.Duration
	UnhealthyThreshold float64
	JitterMin          time.Duration
	JitterMax          time.Duration
	StalenessPenalty   uint64
	BlockAwareCache    bool

	// Optional callbacks for logging/metrics
	OnWin     func(url string, latency time.Duration, method string)
	OnFail    func(url, method string, err error)
	OnBreaker func(url string, open bool)
}

func defaults() Config {
	return Config{
		PerAttemptTimeout:  4 * time.Second,
		TotalBudget:        30 * time.Second,
		MaxRetries:         5,
		ColdFanout:         2,
		WarmFanout:         1,
		PromoteWins:        2,
		StickyTTL:          60 * time.Second,
		CallTTL:            2 * time.Second,
		MaxCacheEntries:    2000,
		EWMAAlpha:          0.25,
		CooldownDuration:   15 * time.Second,
		UnhealthyThreshold: 0.65,
		JitterMin:          50 * time.Millisecond,
		JitterMax:          300 * time.Millisecond,
		StalenessPenalty:   2,
		BlockAwareCache:    true,
	}
}

func merge(in Config) Config {
	d := defaults()
	if in.PerAttemptTimeout > 0 {
		d.PerAttemptTimeout = in.PerAttemptTimeout
	}
	if in.TotalBudget > 0 {
		d.TotalBudget = in.TotalBudget
	}
	if in.MaxRetries > 0 {
		d.MaxRetries = in.MaxRetries
	}
	if in.ColdFanout > 0 {
		d.ColdFanout = in.ColdFanout
	}
	if in.WarmFanout > 0 {
		d.WarmFanout = in.WarmFanout
	}
	if in.PromoteWins > 0 {
		d.PromoteWins = in.PromoteWins
	}
	if in.StickyTTL > 0 {
		d.StickyTTL = in.StickyTTL
	}
	if in.CallTTL > 0 {
		d.CallTTL = in.CallTTL
	}
	if in.MaxCacheEntries > 0 {
		d.MaxCacheEntries = in.MaxCacheEntries
	}
	if in.EWMAAlpha > 0 {
		d.EWMAAlpha = in.EWMAAlpha
	}
	if in.CooldownDuration > 0 {
		d.CooldownDuration = in.CooldownDuration
	}
	if in.UnhealthyThreshold > 0 {
		d.UnhealthyThreshold = in.UnhealthyThreshold
	}
	if in.JitterMin > 0 {
		d.JitterMin = in.JitterMin
	}
	if in.JitterMax > 0 {
		d.JitterMax = in.JitterMax
	}
	if in.StalenessPenalty > 0 {
		d.StalenessPenalty = in.StalenessPenalty
	}
	d.BlockAwareCache = in.BlockAwareCache
	d.Pool = in.Pool
	d.OnWin = in.OnWin
	d.OnFail = in.OnFail
	d.OnBreaker = in.OnBreaker
	return d
}

// ---------------------------------------------------------------------------
// Endpoint state
// ---------------------------------------------------------------------------

type endpoint struct {
	url             string
	latEWMA         float64 // ms
	errEWMA         float64
	wins            int
	breakerUntil    time.Time
	lastBlockSeen   uint64
	lastBlockSeenAt time.Time
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	errRaceLost  = errors.New("race lost")
	errBudget    = errors.New("budget exceeded")
	errAllFailed = errors.New("all endpoints failed")
	errNoHealthy = errors.New("no healthy endpoints")
	errBalancer  = errors.New("balancer error")
)

type rpcError struct {
	httpCode   int
	rateLimit  bool
	retryAfter time.Duration
	rpcMsg     string
}

func (e *rpcError) Error() string {
	if e.rpcMsg != "" {
		return fmt.Sprintf("rpc error: %s", e.rpcMsg)
	}
	return fmt.Sprintf("http %d", e.httpCode)
}

// ---------------------------------------------------------------------------
// Balancer
// ---------------------------------------------------------------------------

type Balancer struct {
	cfg    Config
	client *http.Client

	mu            sync.RWMutex
	endpoints     []*endpoint
	stickyURL     string
	stickyExpires time.Time
	lastBlock     uint64
	lastBlockAt   time.Time

	cacheMu sync.Mutex
	cache   map[string]cacheEntry

	sf singleflight.Group
}

type cacheEntry struct {
	value   json.RawMessage
	expires time.Time
}

// New creates a balancer. Pool must be non-empty.
func New(cfg Config) *Balancer {
	cfg = merge(cfg)
	if len(cfg.Pool) == 0 {
		panic("transport: empty pool")
	}

	// Tuned HTTP client — connection pooling, http/2, keep-alive.
	// This replaces the localStorage breakers + browser preconnect from the TS version.
	tr := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		ForceAttemptHTTP2:   true,
		DisableCompression:  false,
	}

	b := &Balancer{
		cfg:    cfg,
		client: &http.Client{Transport: tr},
		cache:  make(map[string]cacheEntry, 256),
	}
	b.endpoints = make([]*endpoint, len(cfg.Pool))
	for i, u := range cfg.Pool {
		b.endpoints[i] = &endpoint{url: u, latEWMA: 800}
	}
	return b
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

func (b *Balancer) score(e *endpoint, tip uint64) float64 {
	lat := 1.0 / (1.0 + e.latEWMA)
	ok := 1.0 - e.errEWMA
	if ok < 0 {
		ok = 0
	}
	healthy := 1.0
	if time.Now().Before(e.breakerUntil) {
		healthy = 0.01
	}
	freshness := 1.0
	if tip > 0 && e.lastBlockSeen > 0 && time.Since(e.lastBlockSeenAt) < 30*time.Second {
		if e.lastBlockSeen+b.cfg.StalenessPenalty < tip {
			behind := float64(tip - e.lastBlockSeen)
			freshness = 1.0 / (1.0 + (behind-float64(b.cfg.StalenessPenalty))*0.5)
			if freshness < 0.05 {
				freshness = 0.05
			}
		}
	}
	return lat * ok * healthy * freshness
}

func (b *Balancer) weightedSample(pool []*endpoint, k int, tip uint64) []*endpoint {
	if k > len(pool) {
		k = len(pool)
	}
	if k <= 0 {
		return nil
	}
	items := make([]*endpoint, len(pool))
	copy(items, pool)
	out := make([]*endpoint, 0, k)

	for i := 0; i < k && len(items) > 0; i++ {
		total := 0.0
		for _, it := range items {
			total += b.score(it, tip)
		}
		r := rand.Float64() * total
		idx := 0
		for idx < len(items) {
			r -= b.score(items[idx], tip)
			if r <= 0 {
				break
			}
			idx++
		}
		if idx >= len(items) {
			idx = len(items) - 1
		}
		out = append(out, items[idx])
		items = append(items[:idx], items[idx+1:]...)
	}
	return out
}

// ---------------------------------------------------------------------------
// Record win/fail
// ---------------------------------------------------------------------------

func (b *Balancer) recordWin(url string, latency time.Duration, method string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range b.endpoints {
		if e.url != url {
			continue
		}
		latMs := float64(latency.Milliseconds())
		e.latEWMA = e.latEWMA*(1-b.cfg.EWMAAlpha) + latMs*b.cfg.EWMAAlpha
		e.errEWMA = e.errEWMA * (1 - b.cfg.EWMAAlpha)
		if b.stickyURL == url {
			e.wins++
		} else {
			e.wins = 1
		}
		if !e.breakerUntil.IsZero() {
			e.breakerUntil = time.Time{}
			if b.cfg.OnBreaker != nil {
				go b.cfg.OnBreaker(url, false)
			}
		}
		break
	}
	b.stickyURL = url
	b.stickyExpires = time.Now().Add(b.cfg.StickyTTL)
	if b.cfg.OnWin != nil {
		go b.cfg.OnWin(url, latency, method)
	}
}

func (b *Balancer) recordFail(url string, method string, err error) {
	// Don't penalize for "we cancelled you because someone else won"
	if errors.Is(err, errRaceLost) || errors.Is(err, errBudget) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range b.endpoints {
		if e.url != url {
			continue
		}
		e.errEWMA = e.errEWMA*(1-b.cfg.EWMAAlpha) + 1.0*b.cfg.EWMAAlpha
		e.wins = 0
		var rpcErr *rpcError
		if errors.As(err, &rpcErr) && rpcErr.rateLimit && rpcErr.retryAfter > 0 {
			cd := rpcErr.retryAfter
			if cd > 5*time.Minute {
				cd = 5 * time.Minute
			}
			e.breakerUntil = time.Now().Add(cd)
			if b.cfg.OnBreaker != nil {
				go b.cfg.OnBreaker(url, true)
			}
		} else if e.errEWMA >= b.cfg.UnhealthyThreshold {
			e.breakerUntil = time.Now().Add(b.cfg.CooldownDuration)
			if b.cfg.OnBreaker != nil {
				go b.cfg.OnBreaker(url, true)
			}
		}
		break
	}
	if b.stickyURL == url {
		b.stickyURL = ""
	}
	if b.cfg.OnFail != nil {
		go b.cfg.OnFail(url, method, err)
	}
}

// ---------------------------------------------------------------------------
// Block tracking (for staleness scoring + cache invalidation)
// ---------------------------------------------------------------------------

func (b *Balancer) recordBlockResp(url, method string, result json.RawMessage) {
	if method != "eth_blockNumber" {
		return
	}
	var hex string
	if err := json.Unmarshal(result, &hex); err != nil {
		return
	}
	hex = strings.TrimPrefix(hex, "0x")
	n, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return
	}
	b.mu.Lock()
	for _, e := range b.endpoints {
		if e.url == url {
			e.lastBlockSeen = n
			e.lastBlockSeenAt = time.Now()
			break
		}
	}
	prev := b.lastBlock
	if n > b.lastBlock {
		b.lastBlock = n
		b.lastBlockAt = time.Now()
	}
	b.mu.Unlock()

	// New block → drop cache (block-aware invalidation)
	if b.cfg.BlockAwareCache && n != prev {
		b.cacheMu.Lock()
		b.cache = make(map[string]cacheEntry, 256)
		b.cacheMu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Choose racers
// ---------------------------------------------------------------------------

func (b *Balancer) chooseRacers(method string) []*endpoint {
	b.mu.RLock()
	defer b.mu.RUnlock()

	warm := b.stickyURL != "" && time.Now().Before(b.stickyExpires)
	var stickyEP *endpoint
	if warm {
		for _, e := range b.endpoints {
			if e.url == b.stickyURL && e.wins >= b.cfg.PromoteWins {
				stickyEP = e
				break
			}
		}
		warm = stickyEP != nil
	}
	fanout := b.cfg.ColdFanout
	if warm {
		fanout = b.cfg.WarmFanout
	}

	healthy := make([]*endpoint, 0, len(b.endpoints))
	for _, e := range b.endpoints {
		if time.Now().After(e.breakerUntil) {
			healthy = append(healthy, e)
		}
	}
	pool := healthy
	if len(pool) == 0 {
		pool = b.endpoints // all dead, race them anyway as last resort
	}

	if warm && stickyEP != nil && time.Now().After(stickyEP.breakerUntil) {
		rest := make([]*endpoint, 0, len(pool)-1)
		for _, e := range pool {
			if e != stickyEP {
				rest = append(rest, e)
			}
		}
		picks := []*endpoint{stickyEP}
		picks = append(picks, b.weightedSample(rest, fanout-1, b.lastBlock)...)
		return picks
	}
	return b.weightedSample(pool, fanout, b.lastBlock)
}

// ---------------------------------------------------------------------------
// Single HTTP request
// ---------------------------------------------------------------------------

type jsonrpcReq struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type jsonrpcResp struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (b *Balancer) doRequest(ctx context.Context, url, method string, params any) (json.RawMessage, error) {
	body, _ := json.Marshal(jsonrpcReq{
		Jsonrpc: "2.0",
		ID:      rand.Int63(),
		Method:  method,
		Params:  params,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		retry := 60 * time.Second
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if n, err := strconv.Atoi(ra); err == nil && n > 0 {
				retry = time.Duration(n) * time.Second
			}
		}
		return nil, &rpcError{httpCode: 429, rateLimit: true, retryAfter: retry}
	}
	if resp.StatusCode != 200 {
		return nil, &rpcError{httpCode: resp.StatusCode}
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var rr jsonrpcResp
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, err
	}
	if rr.Error != nil {
		return nil, &rpcError{rpcMsg: rr.Error.Message}
	}
	return rr.Result, nil
}

// ---------------------------------------------------------------------------
// Race (hedged request)
// ---------------------------------------------------------------------------

type raceResult struct {
	url     string
	latency time.Duration
	result  json.RawMessage
	err     error
}

func (b *Balancer) raceOnce(parentCtx context.Context, racers []*endpoint, method string, params any) (json.RawMessage, string, time.Duration, error) {
	ctx, cancel := context.WithTimeout(parentCtx, b.cfg.PerAttemptTimeout)
	defer cancel()

	resultCh := make(chan raceResult, len(racers))
	for _, ep := range racers {
		go func(ep *endpoint) {
			t0 := time.Now()
			res, err := b.doRequest(ctx, ep.url, method, params)
			resultCh <- raceResult{
				url:     ep.url,
				latency: time.Since(t0),
				result:  res,
				err:     err,
			}
		}(ep)
	}

	var lastErr error
	losers := 0
	for i := 0; i < len(racers); i++ {
		select {
		case rr := <-resultCh:
			if rr.err == nil {
				// Winner!
				b.recordWin(rr.url, rr.latency, method)
				b.recordBlockResp(rr.url, method, rr.result)
				cancel() // cancel siblings
				// Record losers as "race lost" so they don't get penalized
				go func(n int) {
					for j := 0; j < n; j++ {
						lr := <-resultCh
						if lr.err == nil {
							continue
						}
						b.recordFail(lr.url, method, errRaceLost)
					}
				}(len(racers) - i - 1)
				return rr.result, rr.url, rr.latency, nil
			}
			b.recordFail(rr.url, method, rr.err)
			lastErr = rr.err
			losers++
		case <-ctx.Done():
			return nil, "", 0, ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = errAllFailed
	}
	return nil, "", 0, lastErr
}

func (b *Balancer) racePool(ctx context.Context, method string, params any) (json.RawMessage, error) {
	deadline := time.Now().Add(b.cfg.TotalBudget)
	budgetCtx, cancelBudget := context.WithDeadline(ctx, deadline)
	defer cancelBudget()

	var lastErr error
	for attempt := 0; attempt < b.cfg.MaxRetries; attempt++ {
		if time.Now().After(deadline) {
			break
		}
		if attempt > 0 {
			jit := b.cfg.JitterMin + time.Duration(rand.Int63n(int64(b.cfg.JitterMax-b.cfg.JitterMin)))
			select {
			case <-time.After(jit):
			case <-budgetCtx.Done():
				return nil, budgetCtx.Err()
			}
		}
		racers := b.chooseRacers(method)
		if len(racers) == 0 {
			return nil, errNoHealthy
		}
		res, _, _, err := b.raceOnce(budgetCtx, racers, method, params)
		if err == nil {
			return res, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errAllFailed
	}
	return nil, lastErr
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

var neverCache = map[string]bool{
	"eth_sendTransaction":      true,
	"eth_sendRawTransaction":   true,
	"eth_estimateGas":          true,
	"eth_gasPrice":             true,
	"eth_maxPriorityFeePerGas": true,
	"eth_feeHistory":           true,
}

func (b *Balancer) cacheKey(method string, params any) string {
	pb, _ := json.Marshal(params)
	return method + "|" + string(pb)
}

func (b *Balancer) getCache(key string) (json.RawMessage, bool) {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	e, ok := b.cache[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.value, true
}

func (b *Balancer) putCache(key string, val json.RawMessage) {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if len(b.cache) >= b.cfg.MaxCacheEntries {
		// Cheap eviction: drop a random ~10% of entries
		drop := b.cfg.MaxCacheEntries / 10
		i := 0
		for k := range b.cache {
			if i >= drop {
				break
			}
			delete(b.cache, k)
			i++
		}
	}
	b.cache[key] = cacheEntry{
		value:   val,
		expires: time.Now().Add(b.cfg.CallTTL),
	}
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Call invokes a JSON-RPC method through the balancer.
// The result is unmarshaled into `out` (pass a pointer).
func (b *Balancer) Call(ctx context.Context, method string, params any, out any) error {
	raw, err := b.CallRaw(ctx, method, params)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// CallRaw returns the raw JSON result from the RPC.
func (b *Balancer) CallRaw(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if neverCache[method] {
		return b.racePool(ctx, method, params)
	}

	key := b.cacheKey(method, params)
	if v, ok := b.getCache(key); ok {
		return v, nil
	}

	// singleflight: concurrent identical calls collapse to one network request.
	// This is the go equivalent of the JS `inflight` map.
	v, err, _ := b.sf.Do(key, func() (any, error) {
		// Recheck cache after acquiring singleflight slot
		if cached, ok := b.getCache(key); ok {
			return cached, nil
		}
		raw, err := b.racePool(ctx, method, params)
		if err != nil {
			return nil, err
		}
		b.putCache(key, raw)
		return raw, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(json.RawMessage), nil
}
