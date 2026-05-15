package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

const (
	apiChallengesURL = "https://api.frankencoin.com/challenges/list"
	apiBidsURL       = "https://api.frankencoin.com/challenges/bids/list"
	bootChalMarker   = "bootstrap_chal_block"
)

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

// BootstrapChallenges seeds challenges + bids from api.frankencoin.com once.
// V2-only. Sets the tail marker to current head — tick takes over from there.
//
// Failure modes:
//   - Empty store + API down → set marker to head, tick watches from now.
//     Currently zero active challenges so this loses nothing.
//   - Empty store + API up → seed historical V2 data, set marker.
//   - Already seeded (marker set) → no-op.
func BootstrapChallenges(ctx context.Context, st *store.Store, ch *chain.Client) error {
	if v, _ := st.GetMeta(bootChalMarker); v != "" {
		return nil // already bootstrapped, tick resumes from marker
	}

	// Always set the marker to head when we exit successfully, even if the
	// API fetch fails — that way the tick scans forward from now instead of
	// re-fetching from 2024.
	head, err := ch.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap chal head: %w", err)
	}

	t0 := time.Now()
	chalCount, bidCount, apiErr := seedFromAPI(ctx, st)
	if apiErr != nil {
		fmt.Printf("[bootstrap-chal] api fetch failed (%v) — starting fresh from block %d\n", apiErr, head)
	} else {
		fmt.Printf("[bootstrap-chal] seeded %d challenges, %d bids in %v (V2 only)\n",
			chalCount, bidCount, time.Since(t0))
	}

	return st.SetMeta(bootChalMarker, strconv.FormatUint(head, 10))
}

func seedFromAPI(ctx context.Context, st *store.Store) (int, int, error) {
	chalList, err := fetchChallenges(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("challenges: %w", err)
	}
	bidList, err := fetchBids(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("bids: %w", err)
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
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "GET", apiChallengesURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
	var out apiChallengeList
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out.List, nil
}

func fetchBids(ctx context.Context) ([]apiBid, error) {
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "GET", apiBidsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
	var out apiBidList
	if err := json.Unmarshal(body, &out); err != nil {
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
