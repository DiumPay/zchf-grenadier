package api

import (
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cache"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/helmet"
	"github.com/gofiber/fiber/v3/middleware/limiter"
	"github.com/gofiber/fiber/v3/middleware/recover"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

type Server struct {
	app *fiber.App
	st  *store.Store
	pc  *chain.PriceCache
	// callback to get last scanned block (avoids circular import to indexer)
	lastBlockFn func() uint64
	// trusted proxy hops: 1 for caddy only, 2 for cloudflare -> caddy
	proxyHops int
	// CIDRs whose forwarded-IP headers we trust. Empty = trust loopback only.
	// Configure via GRENADIER_TRUSTED_PROXIES="10.0.0.0/8,172.16.0.0/12".
	trustedProxies []*net.IPNet
}

func New(st *store.Store, pc *chain.PriceCache, lastBlockFn func() uint64) *Server {
	app := fiber.New(fiber.Config{
		AppName:       "grenadier",
		StrictRouting: false,
		CaseSensitive: false,
		ReadTimeout:   15 * time.Second,
		WriteTimeout:  15 * time.Second,
		IdleTimeout:   60 * time.Second,
		BodyLimit:     1 << 20, // 1mb. we accept no bodies anyway.
		ProxyHeader:   fiber.HeaderXForwardedFor,
	})

	s := &Server{
		app:            app,
		st:             st,
		pc:             pc,
		lastBlockFn:    lastBlockFn,
		proxyHops:      atoiDefault(os.Getenv("GRENADIER_PROXY_HOPS"), 1),
		trustedProxies: defaultTrustedProxies(),
	}
	s.routes()
	return s
}

// defaultTrustedProxies: zero-config trust list covering the deployments we
// actually see — Cloudflare in front, Caddy/nginx on the same docker network,
// reverse proxy on the same host. Operator can replace via GRENADIER_TRUSTED_PROXIES
// (comma-separated CIDRs); otherwise this just works.
func defaultTrustedProxies() []*net.IPNet {
	if env := os.Getenv("GRENADIER_TRUSTED_PROXIES"); env != "" {
		return parseCIDRs(env)
	}
	// RFC1918 private + loopback + Cloudflare published ranges (v4 + v6).
	// Cloudflare list: https://www.cloudflare.com/ips/ — these change rarely
	// (last update years apart); refresh by setting the env var if needed.
	return parseCIDRs(strings.Join([]string{
		// Private / loopback / link-local
		"127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
		"169.254.0.0/16", "::1/128", "fc00::/7", "fe80::/10",
		// Cloudflare v4
		"173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22",
		"103.31.4.0/22", "141.101.64.0/18", "108.162.192.0/18",
		"190.93.240.0/20", "188.114.96.0/20", "197.234.240.0/22",
		"198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13",
		"104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22",
		// Cloudflare v6
		"2400:cb00::/32", "2606:4700::/32", "2803:f800::/32",
		"2405:b500::/32", "2405:8100::/32", "2a06:98c0::/29",
		"2c0f:f248::/32",
	}, ","))
}

func (s *Server) routes() {
	// panic recovery: one bad request can't crash the server. Stack handler
	// surfaces what would otherwise be a silent 500.
	s.app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
		StackTraceHandler: func(c fiber.Ctx, e any) {
			fmt.Printf("[panic] %s %s: %v\n%s\n", c.Method(), c.Path(), e, debug.Stack())
		},
	}))

	// Strip Server header — minor obscurity, costs nothing.
	s.app.Use(func(c fiber.Ctx) error {
		c.Response().Header.Del("Server")
		return c.Next()
	})

	// security headers
	s.app.Use(helmet.New(helmet.Config{
		XSSProtection:             "0",
		ContentTypeNosniff:        "nosniff",
		XFrameOptions:             "DENY",
		ReferrerPolicy:            "no-referrer",
		CrossOriginEmbedderPolicy: "require-corp",
		CrossOriginOpenerPolicy:   "same-origin",
		CrossOriginResourcePolicy: "cross-origin",
	}))

	// CORS — public read API. POST is allowed too, but only the /near/* routes
	// accept POST per the method allowlist below.
	s.app.Use(cors.New(cors.Config{
		AllowOrigins:  []string{"*"},
		AllowMethods:  []string{"GET", "HEAD", "OPTIONS", "POST"},
		AllowHeaders:  []string{"Content-Type"},
		ExposeHeaders: []string{"Content-Length"},
		MaxAge:        86400,
	}))

	// URL guard: cheap pre-handler check. Anything pathological in path or
	// query gets a 414 before we touch routing, CORS, or the limiter.
	s.app.Use(func(c fiber.Ctx) error {
		if len(c.Path())+len(c.Request().URI().QueryString()) > 2048 {
			return c.Status(414).JSON(fiber.Map{"error": "uri too long"})
		}
		return c.Next()
	})

	// method allowlist. /near/* allows POST since the NEAR proxy needs it.
	s.app.Use(func(c fiber.Ctx) error {
		m := c.Method()
		if m == "GET" || m == "HEAD" || m == "OPTIONS" {
			return c.Next()
		}
		if m == "POST" && strings.HasPrefix(c.Path(), "/near/") {
			return c.Next()
		}
		return c.Status(405).JSON(fiber.Map{"error": "method not allowed"})
	})

	// Rate limiting: two layers, both keyed on real client IP.
	//   - Burst:     20 req per 5s   → tolerates a page load hitting multiple endpoints
	//   - Sustained: 300 req per 5m  → caps long-running scrapers below ~1 req/s avg
	// Bots that get past one limit hit the other. Genuine users stay well under both.
	s.app.Use(limiter.New(limiter.Config{
		Max:        20,
		Expiration: 5 * time.Second,
		KeyGenerator: func(c fiber.Ctx) string {
			return "burst:" + s.clientIP(c)
		},
		LimitReached: func(c fiber.Ctx) error {
			c.Set("Retry-After", "5")
			return c.Status(429).JSON(fiber.Map{"error": "rate limited (burst)"})
		},
	}))
	s.app.Use(limiter.New(limiter.Config{
		Max:        300,
		Expiration: 5 * time.Minute,
		KeyGenerator: func(c fiber.Ctx) string {
			return "sustained:" + s.clientIP(c)
		},
		LimitReached: func(c fiber.Ctx) error {
			c.Set("Retry-After", "60")
			return c.Status(429).JSON(fiber.Map{"error": "rate limited (sustained)"})
		},
	}))

	// Per-route response caches. TTL chosen by how often the underlying data
	// actually changes (see comments below). MaxBytes caps memory so a
	// flood of unique cache keys (e.g. per-owner) can't grow unbounded.
	const cacheBudget = 16 * 1024 * 1024 // 16 MiB per route
	mk := func(ttl time.Duration) fiber.Handler {
		return cache.New(cache.Config{
			Expiration: ttl,
			MaxBytes:   cacheBudget,
			Methods:    []string{fiber.MethodGet, fiber.MethodHead},
		})
	}
	//   - curated:    rare (new position = governance event), 2 min
	//   - positions:  occasional updates, 30s
	//   - owner:      user wants their own changes visible quickly, 15s
	//   - challenges: zero turnover today, 60s
	//   - bids:       append-only history, 60s
	//   - prices:     no http cache — PriceCache already holds 60s in memory
	//   - health:     no cache — monitors need ground truth
	//   - governance: 5 min upstream refresh; 60s here is fresh-enough.
	cacheCurated := mk(120 * time.Second)
	cachePositions := mk(30 * time.Second)
	cacheOwner := mk(15 * time.Second)
	cacheChalBids := mk(60 * time.Second)
	cacheGovernance := mk(60 * time.Second)

	s.app.Get("/health", s.handleHealth)
	// Public robots.txt — block all polite bots from the API surface.
	s.app.Get("/robots.txt", handleRobotsTxt)
	s.app.Get("/positions", cachePositions, s.handleAllPositions)
	s.app.Get("/positions/curated", cacheCurated, s.handleCurated)
	s.app.Get("/positions/monitored", cachePositions, s.handleMonitored)
	s.app.Get("/positions/owner/:addr", cacheOwner, s.handleByOwner)
	s.app.Get("/challenges", cacheChalBids, s.handleAllChallenges)
	s.app.Get("/challenges/active", cacheChalBids, s.handleActiveChallenges)
	s.app.Get("/challenges/challenger/:addr", cacheChalBids, s.handleChallengesByChallenger)
	s.app.Get("/challenges/position/:addr", cacheChalBids, s.handleChallengesByPosition)
	s.app.Get("/bids/bidder/:addr", cacheChalBids, s.handleBidsByBidder)
	s.app.Get("/bids/position/:addr", cacheChalBids, s.handleBidsByPosition)
	s.app.Get("/prices/list", s.handlePricesList)
	s.app.Get("/prices/ticker/:sym", s.handlePriceTicker)
	// Governance — single composite endpoint by default, plus drill-downs.
	s.app.Get("/governance", cacheGovernance, s.handleGovernance)
	s.app.Get("/governance/minters", cacheGovernance, s.handleGovernanceMinters)
	s.app.Get("/governance/leadrate", cacheGovernance, s.handleGovernanceLeadrate)
	s.app.Get("/governance/fps-holders", cacheGovernance, s.handleGovernanceFPSHolders)
	s.app.Get("/governance/delegations", cacheGovernance, s.handleGovernanceDelegations)

	// NEAR 1Click proxy — opt-in via NEAR_JWT env var.
	// Without a JWT set, /near/quote 404s. Operator-friendly: no flags to set,
	// presence of the token IS the enable signal.
	if jwt := nearJWT(); jwt != "" {
		s.registerNearRoutes(jwt)
		fmt.Println("[api] near 1click proxy enabled at POST /near/quote")
	}

	// catch-all 404
	s.app.Use(func(c fiber.Ctx) error {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	})
}

func (s *Server) Listen(port int) error {
	host := os.Getenv("GRENADIER_BIND")
	if host == "" {
		host = "127.0.0.1"
	}
	return s.app.Listen(fmt.Sprintf("%s:%d", host, port))
}

func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

func (s *Server) handleHealth(c fiber.Ctx) error {
	count, _ := s.st.Count()
	chalCount, _ := s.st.ChallengeCount()
	bidCount, _ := s.st.BidCount()
	minterCount, _ := s.st.MinterCount()
	fpsCount, _ := s.st.FPSHolderCount()
	delegationCount, _ := s.st.DelegationCount()
	c.Set("Cache-Control", "no-store")
	return c.JSON(fiber.Map{
		"ok":          true,
		"positions":   count,
		"challenges":  chalCount,
		"bids":        bidCount,
		"minters":     minterCount,
		"fpsHolders":  fpsCount,
		"delegations": delegationCount,
		"lastBlock":   s.lastBlockFn(),
	})
}

func (s *Server) handleAllPositions(c fiber.Ctx) error {
	// Use AllRaw: the stored blobs are already the same JSON we'd emit, so
	// we skip Position unmarshal + re-marshal entirely for this endpoint.
	rows, err := s.st.AllRaw()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"num":  len(rows),
		"list": rows,
	})
}

func (s *Server) handleCurated(c fiber.Ctx) error {
	positions, err := s.st.Curated()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"num":  len(positions),
		"list": positions,
	})
}

func (s *Server) handleMonitored(c fiber.Ctx) error {
	positions, err := s.st.Monitored()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"num":  len(positions),
		"list": positions,
	})
}

func (s *Server) handleByOwner(c fiber.Ctx) error {
	addr := strings.ToLower(c.Params("addr"))
	if !isAddrLike(addr) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid address"})
	}
	positions, err := s.st.ByOwner(addr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"num":  len(positions),
		"list": positions,
	})
}

// ---- challenges ----

func (s *Server) handleAllChallenges(c fiber.Ctx) error {
	list, err := s.st.AllChallenges()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleActiveChallenges(c fiber.Ctx) error {
	list, err := s.st.ActiveChallenges()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleChallengesByChallenger(c fiber.Ctx) error {
	addr := strings.ToLower(c.Params("addr"))
	if !isAddrLike(addr) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid address"})
	}
	list, err := s.st.ChallengesByChallenger(addr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleChallengesByPosition(c fiber.Ctx) error {
	addr := strings.ToLower(c.Params("addr"))
	if !isAddrLike(addr) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid address"})
	}
	list, err := s.st.ChallengesByPosition(addr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

// ---- bids ----

func (s *Server) handleBidsByBidder(c fiber.Ctx) error {
	addr := strings.ToLower(c.Params("addr"))
	if !isAddrLike(addr) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid address"})
	}
	list, err := s.st.BidsByBidder(addr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleBidsByPosition(c fiber.Ctx) error {
	addr := strings.ToLower(c.Params("addr"))
	if !isAddrLike(addr) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid address"})
	}
	list, err := s.st.BidsByPosition(addr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

// ---- prices (lazy passthrough from api.frankencoin.com for the 5 illiquid tokens) ----

func (s *Server) handlePricesList(c fiber.Ctx) error {
	m := s.pc.All(c.Context())
	list := make([]*chain.Price, 0, len(m))
	for _, p := range m {
		list = append(list, p)
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handlePriceTicker(c fiber.Ctx) error {
	sym := strings.ToUpper(c.Params("sym"))
	p := s.pc.Get(c.Context(), sym)
	if p == nil {
		return c.Status(404).JSON(fiber.Map{"error": "ticker not tracked or unavailable"})
	}
	return c.JSON(p)
}

// ---- governance ----

// handleGovernance is a one-shot endpoint that returns everything the
// frontend needs to render the page in a single round-trip. The pieces are
// also exposed individually below for clients that want finer control.
func (s *Server) handleGovernance(c fiber.Ctx) error {
	minters, err := s.st.AllMinters()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	approved, err := s.st.AllLeadrateApproved()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	proposed, err := s.st.AllLeadrateProposed()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	holders, err := s.st.FPSHoldersTop(20)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	delegations, err := s.st.AllDelegations()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"minters": fiber.Map{
			"num":  len(minters),
			"list": minters,
		},
		"leadrate": fiber.Map{
			"approved": fiber.Map{"num": len(approved), "list": approved},
			"proposed": fiber.Map{"num": len(proposed), "list": proposed},
		},
		"fpsHolders": fiber.Map{
			"num":  len(holders),
			"list": holders,
		},
		"delegations": fiber.Map{
			"num":  len(delegations),
			"list": delegations,
		},
	})
}

func (s *Server) handleGovernanceMinters(c fiber.Ctx) error {
	list, err := s.st.AllMinters()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleGovernanceLeadrate(c fiber.Ctx) error {
	approved, err := s.st.AllLeadrateApproved()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	proposed, err := s.st.AllLeadrateProposed()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"approved": fiber.Map{"num": len(approved), "list": approved},
		"proposed": fiber.Map{"num": len(proposed), "list": proposed},
	})
}

func (s *Server) handleGovernanceFPSHolders(c fiber.Ctx) error {
	list, err := s.st.FPSHoldersTop(20)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

func (s *Server) handleGovernanceDelegations(c fiber.Ctx) error {
	list, err := s.st.AllDelegations()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"num": len(list), "list": list})
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// clientIP returns the real client IP. Forwarded headers are honored only
// when the immediate socket peer is in trustedProxies (Cloudflare CIDRs,
// internal LB, etc). Otherwise an attacker hitting us directly could spoof
// CF-Connecting-IP and bypass rate limits.
func (s *Server) clientIP(c fiber.Ctx) string {
	peer := net.ParseIP(c.IP())
	trusted := peer != nil && peer.IsLoopback()
	if !trusted && peer != nil {
		for _, n := range s.trustedProxies {
			if n.Contains(peer) {
				trusted = true
				break
			}
		}
	}
	if !trusted {
		return c.IP() // ignore forwarded headers from untrusted peers
	}
	if s.proxyHops > 0 {
		if cf := c.Get("CF-Connecting-IP"); cf != "" {
			return cf
		}
		if xr := c.Get("X-Real-IP"); xr != "" {
			return xr
		}
		if xff := c.Get(fiber.HeaderXForwardedFor); xff != "" {
			parts := strings.Split(xff, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			idx := len(parts) - s.proxyHops
			if idx < 0 {
				idx = 0
			}
			if parts[idx] != "" {
				return parts[idx]
			}
		}
	}
	return c.IP()
}

// parseCIDRs parses a comma-separated list of CIDR blocks. Invalid entries
// are skipped silently — operator typo in the env shouldn't crash startup.
func parseCIDRs(s string) []*net.IPNet {
	if s == "" {
		return nil
	}
	var out []*net.IPNet
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(part); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func isAddrLike(s string) bool {
	if !strings.HasPrefix(s, "0x") || len(s) != 42 {
		return false
	}
	for _, c := range s[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
