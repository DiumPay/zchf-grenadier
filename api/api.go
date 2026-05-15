package api

import (
	"fmt"
	"os"
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
		app:         app,
		st:          st,
		pc:          pc,
		lastBlockFn: lastBlockFn,
		proxyHops:   atoiDefault(os.Getenv("GRENADIER_PROXY_HOPS"), 1),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	// panic recovery: one bad request can't crash the server
	s.app.Use(recover.New())

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

	// CORS — public read API
	s.app.Use(cors.New(cors.Config{
		AllowOrigins:  []string{"*"},
		AllowMethods:  []string{"GET", "HEAD", "OPTIONS"},
		AllowHeaders:  []string{"Content-Type"},
		ExposeHeaders: []string{"Content-Length"},
		MaxAge:        86400,
	}))

	// method allowlist
	s.app.Use(func(c fiber.Ctx) error {
		m := c.Method()
		if m != "GET" && m != "HEAD" && m != "OPTIONS" {
			return c.Status(405).JSON(fiber.Map{"error": "method not allowed"})
		}
		return c.Next()
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
	// actually changes:
	//   - curated:    rare (new position = governance event), 2 min
	//   - positions:  occasional updates, 30s
	//   - owner:      user wants their own changes visible quickly, 15s
	//   - challenges: zero turnover today, 60s
	//   - bids:       append-only history, 60s
	//   - prices:     no http cache — PriceCache already holds 60s in memory
	//   - health:     no cache — monitors need ground truth
	cacheCurated := cache.New(cache.Config{Expiration: 120 * time.Second, Methods: []string{fiber.MethodGet, fiber.MethodHead}})
	cachePositions := cache.New(cache.Config{Expiration: 30 * time.Second, Methods: []string{fiber.MethodGet, fiber.MethodHead}})
	cacheOwner := cache.New(cache.Config{Expiration: 15 * time.Second, Methods: []string{fiber.MethodGet, fiber.MethodHead}})
	cacheChalBids := cache.New(cache.Config{Expiration: 60 * time.Second, Methods: []string{fiber.MethodGet, fiber.MethodHead}})
	// Governance data refreshes every 5 min upstream, so a 60s HTTP cache
	// gives us "fresh enough" without hammering the DB for repeated reads.
	cacheGovernance := cache.New(cache.Config{Expiration: 60 * time.Second, Methods: []string{fiber.MethodGet, fiber.MethodHead}})

	s.app.Get("/health", s.handleHealth)
	s.app.Get("/positions", cachePositions, s.handleAllPositions)
	s.app.Get("/positions/curated", cacheCurated, s.handleCurated)
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
	positions, err := s.st.All()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"num":  len(positions),
		"list": positions,
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

// clientIP returns the real client IP, accounting for proxy hops.
// trust order: CF-Connecting-IP > X-Real-IP > X-Forwarded-For[len-hops] > socket
func (s *Server) clientIP(c fiber.Ctx) string {
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
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}
