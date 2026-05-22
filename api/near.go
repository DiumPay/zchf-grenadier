package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/limiter"
)

// NEAR 1Click proxy. Forwards POST /near/quote to 1click.chaindefuser.com/v0/quote
// with the operator's JWT injected server-side, so the token never reaches the
// browser bundle.
//
// Opt-in: route registers only when NEAR_JWT env var is set. Self-hosters
// without a JWT see a 404 on /near/quote, no other change.
//
// Protection model:
//   - Body whitelist: client sends 7 fields, backend reconstructs the upstream
//     body itself. appFees, referral, swapType, etc. are never forwarded from
//     the client. This is what stops "use grenadier's JWT to siphon fees".
//   - originAsset must be one of the 3 USDC asset IDs the frankencoin frontend
//     actually uses. Closes the "use this as a generic NEAR proxy" vector.
//   - amount capped at 200,000 USDC (matches frontend MAX_AMOUNT_WEI).
//   - tier-3 rate limit: 3/10s + 100/hr per IP, on top of global limits.

const (
	nearAPIBase   = "https://1click.chaindefuser.com/v0"
	nearMaxAmount = "200000000000" // 200,000 USDC, 6 decimals
	nearBodyLimit = 4 * 1024       // 4 KiB is generous; real bodies are <1 KiB
)

// allowed origin assets (USDC on Ethereum / Base / Gnosis, as nep141 IDs).
// must match frontend's NEAR_ORIGIN_ASSETS map.
var nearAllowedOriginAssets = map[string]bool{
	"nep141:eth-0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.omft.near":    true, // Ethereum USDC
	"nep141:base-0x833589fcd6edb6e08f4c7c32d4f71b54bda02913.omft.near":   true, // Base USDC
	"nep141:gnosis-0xddafbb505ad214d7b80b1f830fccc89b60fb7a83.omft.near": true, // Gnosis USDC
}

// shared HTTP client. timeouts kept short — NEAR's /quote responds in <3s
// normally; anything over that is a problem worth surfacing.
var nearHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// inbound payload — exactly what the frontend may send.
type nearQuoteReq struct {
	OriginAsset      string `json:"originAsset"`
	DestinationAsset string `json:"destinationAsset"`
	Amount           string `json:"amount"`
	Recipient        string `json:"recipient"`
	RefundTo         string `json:"refundTo"`
	SlippageBps      int    `json:"slippageBps"`
	Deadline         string `json:"deadline"`
}

// outbound payload — the upstream NEAR body, reconstructed from validated
// inbound fields plus hardcoded defaults.
type nearQuoteUpstream struct {
	Dry               bool   `json:"dry"`
	SwapType          string `json:"swapType"`
	SlippageTolerance int    `json:"slippageTolerance"`
	OriginAsset       string `json:"originAsset"`
	DepositType       string `json:"depositType"`
	DestinationAsset  string `json:"destinationAsset"`
	Amount            string `json:"amount"`
	RefundTo          string `json:"refundTo"`
	RefundType        string `json:"refundType"`
	Recipient         string `json:"recipient"`
	RecipientType     string `json:"recipientType"`
	Deadline          string `json:"deadline"`
}

// registerNearRoutes wires the proxy route. Caller must check NEAR_JWT first.
func (s *Server) registerNearRoutes(jwt string) {
	// tier-3 limit: 3/10s burst, on top of global 20/5s + 300/5m.
	burst := limiter.New(limiter.Config{
		Max:        3,
		Expiration: 10 * time.Second,
		KeyGenerator: func(c fiber.Ctx) string {
			return "near-burst:" + s.clientIP(c)
		},
		LimitReached: func(c fiber.Ctx) error {
			c.Set("Retry-After", "10")
			return c.Status(429).JSON(fiber.Map{"error": "rate limited (near burst)"})
		},
	})
	// tier-3 limit: 100/hour cap to bound JWT quota burn per IP.
	hourly := limiter.New(limiter.Config{
		Max:        100,
		Expiration: 1 * time.Hour,
		KeyGenerator: func(c fiber.Ctx) string {
			return "near-hourly:" + s.clientIP(c)
		},
		LimitReached: func(c fiber.Ctx) error {
			c.Set("Retry-After", "3600")
			return c.Status(429).JSON(fiber.Map{"error": "rate limited (near hourly)"})
		},
	})

	s.app.Post("/near/quote", burst, hourly, s.makeNearQuoteHandler(jwt))
}

func (s *Server) makeNearQuoteHandler(jwt string) fiber.Handler {
	return func(c fiber.Ctx) error {
		// hard body cap before parsing — global 1 MiB is overkill here.
		if len(c.Body()) > nearBodyLimit {
			return c.Status(413).JSON(fiber.Map{"error": "body too large"})
		}

		var req nearQuoteReq
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid json"})
		}

		// validate inbound fields. errors are intentionally generic — we don't
		// help attackers learn which field tripped which check.
		if !nearAllowedOriginAssets[req.OriginAsset] {
			return c.Status(400).JSON(fiber.Map{"error": "originAsset not allowed"})
		}
		if !strings.HasPrefix(req.DestinationAsset, "nep141:") || len(req.DestinationAsset) > 200 {
			return c.Status(400).JSON(fiber.Map{"error": "destinationAsset invalid"})
		}
		if !isDigitString(req.Amount) || len(req.Amount) > 30 {
			return c.Status(400).JSON(fiber.Map{"error": "amount invalid"})
		}
		if compareDigitStrings(req.Amount, nearMaxAmount) > 0 {
			return c.Status(400).JSON(fiber.Map{"error": "amount exceeds cap"})
		}
		if req.Recipient == "" || len(req.Recipient) > 200 {
			return c.Status(400).JSON(fiber.Map{"error": "recipient invalid"})
		}
		if req.RefundTo == "" || len(req.RefundTo) > 100 {
			return c.Status(400).JSON(fiber.Map{"error": "refundTo invalid"})
		}
		if req.SlippageBps < 10 || req.SlippageBps > 500 {
			return c.Status(400).JSON(fiber.Map{"error": "slippageBps out of range"})
		}
		if req.Deadline == "" || len(req.Deadline) > 40 {
			return c.Status(400).JSON(fiber.Map{"error": "deadline invalid"})
		}

		// reconstruct upstream body. swapType / depositType / etc. are hardcoded
		// here — client cannot influence them.
		upstream := nearQuoteUpstream{
			Dry:               false,
			SwapType:          "FLEX_INPUT",
			SlippageTolerance: req.SlippageBps,
			OriginAsset:       req.OriginAsset,
			DepositType:       "ORIGIN_CHAIN",
			DestinationAsset:  req.DestinationAsset,
			Amount:            req.Amount,
			RefundTo:          req.RefundTo,
			RefundType:        "ORIGIN_CHAIN",
			Recipient:         req.Recipient,
			RecipientType:     "DESTINATION_CHAIN",
			Deadline:          req.Deadline,
		}
		body, err := json.Marshal(upstream)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "internal"})
		}

		httpReq, err := http.NewRequestWithContext(c.Context(), "POST", nearAPIBase+"/quote", bytes.NewReader(body))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "internal"})
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+jwt)

		resp, err := nearHTTPClient.Do(httpReq)
		if err != nil {
			return c.Status(502).JSON(fiber.Map{"error": "upstream unreachable"})
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		if err != nil {
			return c.Status(502).JSON(fiber.Map{"error": "upstream read failed"})
		}

		// pass through upstream status + body verbatim. response body is JSON.
		c.Set("Cache-Control", "no-store")
		c.Set("Content-Type", "application/json")
		return c.Status(resp.StatusCode).Send(respBody)
	}
}

func handleRobotsTxt(c fiber.Ctx) error {
	c.Set("Content-Type", "text/plain; charset=utf-8")
	c.Set("Cache-Control", "public, max-age=86400")
	return c.SendString("User-agent: *\nDisallow: /\n")
}

// isDigitString returns true if s is a non-empty string of ASCII digits.
func isDigitString(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// compareDigitStrings compares two digit-string integers without parsing to
// bigint. returns -1, 0, 1.
func compareDigitStrings(a, b string) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// nearJWT returns the operator's NEAR 1Click JWT, or "" if not configured.
func nearJWT() string {
	return strings.TrimSpace(os.Getenv("NEAR_JWT"))
}
