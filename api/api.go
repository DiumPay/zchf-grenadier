package api

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"

	"github.com/DiumPay/zchf-grenadier/store"
)

type Server struct {
	app *fiber.App
	st  *store.Store
	// callback to get last scanned block (avoids circular import to indexer)
	lastBlockFn func() uint64
}

func New(st *store.Store, lastBlockFn func() uint64) *Server {
	app := fiber.New(fiber.Config{
		AppName:       "grenadier",
		StrictRouting: false,
		CaseSensitive: false,
	})
	s := &Server{app: app, st: st, lastBlockFn: lastBlockFn}
	s.routes()
	return s
}

func (s *Server) routes() {
	// CORS — allow frontend to fetch from any origin
	s.app.Use(func(c fiber.Ctx) error {
		c.Set("Access-Control-Allow-Origin", "*")
		c.Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Set("Access-Control-Allow-Headers", "Content-Type")
		if c.Method() == "OPTIONS" {
			return c.SendStatus(204)
		}
		return c.Next()
	})

	s.app.Get("/health", s.handleHealth)
	s.app.Get("/positions", s.handleAllPositions)
	s.app.Get("/positions/curated", s.handleCurated)
	s.app.Get("/positions/owner/:addr", s.handleByOwner)
}

func (s *Server) Listen(port int) error {
	addr := fmt.Sprintf(":%d", port)
	return s.app.Listen(addr)
}

func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

func (s *Server) handleHealth(c fiber.Ctx) error {
	count, _ := s.st.Count()
	return c.JSON(fiber.Map{
		"ok":        true,
		"positions": count,
		"lastBlock": s.lastBlockFn(),
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
