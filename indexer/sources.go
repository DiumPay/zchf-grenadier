package indexer

// Bootstrap peer model — modelled on a Bitcoin node fetching from multiple
// peers. Each bootstrap step tries peers in priority order and returns the
// first successful response.
//
// The peer list is hardcoded below. To add a new public grenadier mirror,
// open a PR adding it to `peers`. Order matters — earlier entries are
// tried first, so community mirrors come before the canonical official
// API. This is intentional: the goal is for grenadier instances to
// federate amongst themselves first, with the official API as the safety
// net of last resort.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// PeerKind selects the URL scheme used to build paths.
type PeerKind int

const (
	PeerGrenadier PeerKind = iota // /positions, /challenges, /governance/*
	PeerOfficial                  // /positions/list, /challenges/list, /ecosystem/minter/list, /savings/leadrate/...
	PeerPonder                    // POST GraphQL, for FPS holders + delegations
)

func (k PeerKind) String() string {
	switch k {
	case PeerOfficial:
		return "official"
	case PeerPonder:
		return "ponder"
	default:
		return "grenadier"
	}
}

// Peer is one source of bootstrap data. Base is the origin without a path.
type Peer struct {
	Name string
	Base string
	Kind PeerKind
}

// peers — bootstrap sources tried in order. Add new mirrors at the top.
// Official API and ponder stay last as the final fallbacks.
var peers = []Peer{
	{Name: "grenadier.frankencoin.win", Base: "https://grenadier.frankencoin.win", Kind: PeerGrenadier},
	{Name: "api.frankencoin.com", Base: "https://api.frankencoin.com", Kind: PeerOfficial},
	{Name: "ponder.frankencoin.com", Base: "https://ponder.frankencoin.com", Kind: PeerPonder},
}

// PathSpec maps an abstract endpoint name to the path on each peer kind.
// An empty string means the peer kind doesn't support this endpoint and
// will be skipped during iteration.
type PathSpec struct {
	Grenadier string // path on a grenadier peer
	Official  string // path on api.frankencoin.com
}

// Endpoint specs. Add new ones here as the bootstrap grows.
var (
	EndpointPositions = PathSpec{
		Grenadier: "/positions",
		Official:  "/positions/list",
	}
	EndpointChallenges = PathSpec{
		Grenadier: "/challenges",
		Official:  "/challenges/list",
	}
	EndpointBids = PathSpec{
		Grenadier: "", // grenadier has no bulk-bids endpoint — official only
		Official:  "/challenges/bids/list",
	}
	EndpointMinters = PathSpec{
		Grenadier: "/governance/minters",
		Official:  "/ecosystem/minter/list",
	}
)

// IterPeers returns the configured peer list. Useful when callers need
// per-kind dispatch logic (e.g. ponder uses POST GraphQL, not REST GET).
func IterPeers() []Peer {
	return peers
}

func (p Peer) pathFor(spec PathSpec) string {
	if p.Kind == PeerOfficial {
		return spec.Official
	}
	return spec.Grenadier
}

// FetchFromPeers iterates the peer list and returns the first successful
// response body for the given endpoint. Each request gets `timeout` of
// its own. If every peer fails, the last error is returned along with a
// summary of all attempts.
//
// We don't race peers in parallel — sequential failover keeps logs
// predictable and avoids hammering upstreams.
func FetchFromPeers(ctx context.Context, spec PathSpec, timeout time.Duration) ([]byte, *Peer, error) {
	var lastErr error
	tried := 0

	for i := range peers {
		p := peers[i]
		if p.Kind == PeerPonder {
			continue // ponder uses GraphQL POST; callers handle it explicitly
		}
		path := p.pathFor(spec)
		if path == "" {
			continue
		}
		tried++
		url := p.Base + path
		body, err := httpGetWithTimeout(ctx, url, timeout)
		if err != nil {
			lastErr = fmt.Errorf("peer %s (%s): %w", p.Name, p.Kind, err)
			fmt.Printf("[peer] %s %s -> %v\n", p.Name, path, err)
			continue
		}
		// Sanity check: response must be JSON-shaped. Catches HTML error
		// pages / captive portals that return 200.
		if len(body) > 0 && body[0] != '{' && body[0] != '[' {
			lastErr = fmt.Errorf("peer %s (%s): non-JSON response", p.Name, p.Kind)
			fmt.Printf("[peer] %s %s -> non-JSON\n", p.Name, path)
			continue
		}
		fmt.Printf("[peer] %s %s -> ok (%d bytes)\n", p.Name, path, len(body))
		return body, &p, nil
	}

	if tried == 0 {
		return nil, nil, fmt.Errorf("no peer supports this endpoint")
	}
	return nil, nil, fmt.Errorf("all %d peers failed: %w", tried, lastErr)
}

// FetchAndDecodeFromPeers is FetchFromPeers + json.Unmarshal into `out`.
func FetchAndDecodeFromPeers(ctx context.Context, spec PathSpec, timeout time.Duration, out any) (*Peer, error) {
	body, peer, err := FetchFromPeers(ctx, spec, timeout)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return peer, fmt.Errorf("decode from %s: %w", peer.Name, err)
	}
	return peer, nil
}

func httpGetWithTimeout(ctx context.Context, url string, timeout time.Duration) ([]byte, error) {
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "grenadier/bootstrap")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
