package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

const bootChalMarker = "bootstrap_chal_block"

// apiChallenge mirrors a row from /challenges/list. Number, start, created,
// duration come back as strings — typed as json.Number so we can parse safely.
type apiChallenge struct {
	ID                 string      `json:"id"`
	Position           string      `json:"position"`
	Number             json.Number `json:"number"`
	TxHash             string      `json:"txHash"`
	Challenger         string      `json:"challenger"`
	Start              json.Number `json:"start"`
	Created            json.Number `json:"created"`
	Duration           json.Number `json:"duration"`
	Size               string      `json:"size"`
	LiqPrice           string      `json:"liqPrice"`
	Bids               json.Number `json:"bids"`
	FilledSize         string      `json:"filledSize"`
	AcquiredCollateral string      `json:"acquiredCollateral"`
	Status             string      `json:"status"`
	Version            int         `json:"version"`
}

type apiBid struct {
	ID                 string      `json:"id"`
	Position           string      `json:"position"`
	Number             json.Number `json:"number"`
	NumberBid          json.Number `json:"numberBid"`
	TxHash             string      `json:"txHash"`
	Bidder             string      `json:"bidder"`
	Created            json.Number `json:"created"`
	BidType            string      `json:"bidType"`
	Bid                string      `json:"bid"`
	Price              string      `json:"price"`
	FilledSize         string      `json:"filledSize"`
	AcquiredCollateral string      `json:"acquiredCollateral"`
	ChallengeSize      string      `json:"challengeSize"`
	Version            int         `json:"version"`
}

type apiChallengeList struct {
	Num  int            `json:"num"`
	List []apiChallenge `json:"list"`
}

type apiBidList struct {
	Num  int      `json:"num"`
	List []apiBid `json:"list"`
}

// BootstrapChallenges seeds challenges + bids from the peer list once.
// V2-only. Sets the tail marker to current head — tick takes over from there.
//
// Failure modes:
//   - Empty store + all peers down → marker stays unset; tick keeps running
//     from last_block, and the next process restart retries the seed. We
//     deliberately do NOT advance the marker on failure: that would mean
//     "starting fresh from current head" became permanent the first time
//     peers happened to be unreachable, losing all historical data forever.
//   - Empty store + any peer up → seed historical V2 data, advance marker.
//   - Already seeded (marker set) → no-op.
//
// The live tick (loop.go) gates on last_block, not this marker, so the
// indexer keeps moving forward regardless of bootstrap status.
func BootstrapChallenges(ctx context.Context, st *store.Store, ch *chain.Client) error {
	if v, _ := st.GetMeta(bootChalMarker); v != "" {
		return nil // already bootstrapped, tick resumes from marker
	}

	head, err := ch.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap chal head: %w", err)
	}

	t0 := time.Now()
	chalCount, bidCount, apiErr := seedFromPeers(ctx, st)
	if apiErr != nil {
		fmt.Printf("[bootstrap-chal] peer fetch failed (%v) — leaving marker unset so next startup retries\n", apiErr)
		return nil
	}
	fmt.Printf("[bootstrap-chal] seeded %d challenges, %d bids in %v (V2 only)\n",
		chalCount, bidCount, time.Since(t0))

	return st.SetMeta(bootChalMarker, strconv.FormatUint(head, 10))
}

func seedFromPeers(ctx context.Context, st *store.Store) (int, int, error) {
	chalList, err := fetchChallenges(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("challenges: %w", err)
	}
	bidList, err := fetchBids(ctx)
	if err != nil {
		// Non-fatal: challenges seeded but bids couldn't be. Better than
		// nothing — tick will catch up bids forward from now.
		fmt.Printf("[bootstrap-chal] bids unavailable from peers (%v) — skipping\n", err)
		bidList = nil
	}

	chals := make([]*store.Challenge, 0, len(chalList))
	for i := range chalList {
		c := &chalList[i]
		if c.Version != 2 {
			continue
		}
		chals = append(chals, convertChallenge(c))
	}
	bids := make([]*store.Bid, 0, len(bidList))
	for i := range bidList {
		b := &bidList[i]
		if b.Version != 2 {
			continue
		}
		bids = append(bids, convertBid(b))
	}

	if err := st.BulkUpsertChallenges(chals, 0); err != nil {
		return 0, 0, fmt.Errorf("upsert challenges: %w", err)
	}
	if err := st.BulkUpsertBids(bids); err != nil {
		return len(chals), 0, fmt.Errorf("upsert bids: %w", err)
	}
	return len(chals), len(bids), nil
}

func fetchChallenges(ctx context.Context) ([]apiChallenge, error) {
	var out apiChallengeList
	if _, err := FetchAndDecodeFromPeers(ctx, EndpointChallenges, 30*time.Second, &out); err != nil {
		return nil, err
	}
	return out.List, nil
}

func fetchBids(ctx context.Context) ([]apiBid, error) {
	var out apiBidList
	if _, err := FetchAndDecodeFromPeers(ctx, EndpointBids, 30*time.Second, &out); err != nil {
		return nil, err
	}
	return out.List, nil
}

func convertChallenge(a *apiChallenge) *store.Challenge {
	// API status is "Success" / "Active" / "Averted". Normalize: keep as-is
	// since downstream consumers will see the same string. We mostly care
	// that "Active" is queryable from the index.
	return &store.Challenge{
		ID:                 a.ID,
		Position:           strings.ToLower(a.Position),
		Number:             parseU64(a.Number),
		TxHash:             a.TxHash,
		Challenger:         strings.ToLower(a.Challenger),
		Start:              parseI64(a.Start),
		Created:            parseI64(a.Created),
		Duration:           parseI64(a.Duration),
		Size:               a.Size,
		LiqPrice:           a.LiqPrice,
		Bids:               int(parseU64(a.Bids)),
		FilledSize:         a.FilledSize,
		AcquiredCollateral: a.AcquiredCollateral,
		Status:             a.Status,
		Version:            a.Version,
	}
}

func convertBid(a *apiBid) *store.Bid {
	return &store.Bid{
		ID:                 a.ID,
		Position:           strings.ToLower(a.Position),
		Number:             parseU64(a.Number),
		NumberBid:          parseU64(a.NumberBid),
		TxHash:             a.TxHash,
		Bidder:             strings.ToLower(a.Bidder),
		Created:            parseI64(a.Created),
		BidType:            a.BidType,
		Bid:                a.Bid,
		Price:              a.Price,
		FilledSize:         a.FilledSize,
		AcquiredCollateral: a.AcquiredCollateral,
		ChallengeSize:      a.ChallengeSize,
		Version:            a.Version,
	}
}

func parseU64(n json.Number) uint64 {
	if n == "" {
		return 0
	}
	v, err := strconv.ParseUint(string(n), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseI64(n json.Number) int64 {
	if n == "" {
		return 0
	}
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
