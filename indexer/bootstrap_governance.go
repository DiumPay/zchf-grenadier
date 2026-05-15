package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DiumPay/zchf-grenadier/store"
)

const (
	apiMinterListURL        = "https://api.frankencoin.com/ecosystem/minter/list"
	apiLeadrateRatesURL     = "https://api.frankencoin.com/savings/leadrate/rates"
	apiLeadrateProposalsURL = "https://api.frankencoin.com/savings/leadrate/proposals"
	ponderURL               = "https://ponder.frankencoin.com"

	equityAddress = "0x1ba26788dfde592fec8bcb0eaff472a42be341b2"

	// Hard cap on number of FPS holders we keep. Their UI shows 20; we
	// store a few more for breathing room when computing supporter chains.
	fpsHolderLimit = 50

	// How often the governance refresh loop runs. Data is slow-moving
	// (minter proposals happen weekly at most, leadrate changes monthly).
	governanceRefreshInterval = 5 * time.Minute
)

// BootstrapGovernance does the first synchronous pull. Failures are logged
// but non-fatal — the periodic refresh loop will retry. We deliberately
// don't gate this behind a marker like the challenges bootstrap because
// the snapshot semantics mean re-fetching is harmless and we always want
// to update.
func BootstrapGovernance(ctx context.Context, st *store.Store) error {
	t0 := time.Now()
	if err := refreshGovernanceOnce(ctx, st); err != nil {
		return err
	}
	mc, _ := st.MinterCount()
	la, _ := st.AllLeadrateApproved()
	lp, _ := st.AllLeadrateProposed()
	fh, _ := st.FPSHolderCount()
	dc, _ := st.DelegationCount()
	fmt.Printf("[bootstrap-gov] %d minters, %d approved rates, %d proposed rates, %d FPS holders, %d delegations in %v\n",
		mc, len(la), len(lp), fh, dc, time.Since(t0))
	return nil
}

// RunGovernanceRefresh runs the periodic refresh loop. Tick every 5 minutes.
// Cancelled when ctx is done.
func RunGovernanceRefresh(ctx context.Context, st *store.Store) {
	t := time.NewTicker(governanceRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if err := refreshGovernanceOnce(cctx, st); err != nil {
				fmt.Printf("[refresh-gov] %v\n", err)
			}
			cancel()
		}
	}
}

func refreshGovernanceOnce(ctx context.Context, st *store.Store) error {
	// Fire all four upstream fetches in parallel. Each is independent and
	// we tolerate partial failure: if one source fails we just don't
	// touch its table this tick.
	type result struct {
		name string
		err  error
	}
	results := make(chan result, 4)

	go func() {
		results <- result{"minters", fetchAndStoreMinters(ctx, st)}
	}()
	go func() {
		results <- result{"leadrate", fetchAndStoreLeadrate(ctx, st)}
	}()
	go func() {
		results <- result{"fps-holders", fetchAndStoreFPSHolders(ctx, st)}
	}()
	go func() {
		results <- result{"delegations", fetchAndStoreDelegations(ctx, st)}
	}()

	var firstErr error
	for i := 0; i < 4; i++ {
		r := <-results
		if r.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", r.name, r.err)
		}
	}
	return firstErr
}

// ---------------- minters ----------------

type apiMinterEntry struct {
	ChainID           int     `json:"chainId"`
	TxHash            string  `json:"txHash"`
	Minter            string  `json:"minter"`
	ApplicationPeriod int64   `json:"applicationPeriod"`
	ApplicationFee    string  `json:"applicationFee"`
	ApplyMessage      string  `json:"applyMessage"`
	ApplyDate         int64   `json:"applyDate"`
	Suggestor         string  `json:"suggestor"`
	DenyMessage       *string `json:"denyMessage"`
	DenyDate          *int64  `json:"denyDate"`
	DenyTxHash        *string `json:"denyTxHash"`
	Vetor             *string `json:"vetor"`
}

type apiMinterList struct {
	Num  int              `json:"num"`
	List []apiMinterEntry `json:"list"`
}

func fetchAndStoreMinters(ctx context.Context, st *store.Store) error {
	body, err := httpGetJSON(ctx, apiMinterListURL)
	if err != nil {
		return err
	}
	var resp apiMinterList
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	out := make([]*store.Minter, 0, len(resp.List))
	for i := range resp.List {
		e := &resp.List[i]
		m := &store.Minter{
			ChainID:           e.ChainID,
			TxHash:            e.TxHash,
			Minter:            e.Minter,
			ApplicationPeriod: e.ApplicationPeriod,
			ApplicationFee:    e.ApplicationFee,
			ApplyMessage:      e.ApplyMessage,
			ApplyDate:         e.ApplyDate,
			Suggestor:         e.Suggestor,
		}
		if e.DenyMessage != nil {
			m.DenyMessage = *e.DenyMessage
		}
		if e.DenyDate != nil {
			m.DenyDate = *e.DenyDate
		}
		if e.DenyTxHash != nil {
			m.DenyTxHash = *e.DenyTxHash
		}
		if e.Vetor != nil {
			m.Vetor = *e.Vetor
		}
		out = append(out, m)
	}
	return st.BulkUpsertMinters(out)
}

// ---------------- leadrate ----------------

// The leadrate API returns a deeply nested shape:
//
//	{
//	  "rate":     {chainId: {module: <single approved>}},
//	  "list":     {chainId: {module: [history...]}}    // for approved
//	}
//
//	and for proposals:
//	{
//	  "proposed": {chainId: {module: <single pending>}},
//	  "list":     {chainId: {module: [history...]}}
//	}
//
// We only care about `list` from each — it's a superset of `rate`/`proposed`.

type apiLeadrateRow struct {
	ChainID      int    `json:"chainId"`
	Count        int    `json:"count"`
	Module       string `json:"module"`
	Created      int64  `json:"created"`
	Blockheight  int64  `json:"blockheight"`
	TxHash       string `json:"txHash"`
	ApprovedRate int    `json:"approvedRate"`         // approved endpoint
	Proposer     string `json:"proposer,omitempty"`   // proposed endpoint
	NextChange   int64  `json:"nextChange,omitempty"` // proposed endpoint
	NextRate     int    `json:"nextRate,omitempty"`   // proposed endpoint
}

type apiLeadrateRatesResp struct {
	List map[string]map[string][]apiLeadrateRow `json:"list"`
}

type apiLeadrateProposalsResp struct {
	List map[string]map[string][]apiLeadrateRow `json:"list"`
}

func fetchAndStoreLeadrate(ctx context.Context, st *store.Store) error {
	// Fetch both endpoints in parallel.
	type fetchResult struct {
		body []byte
		err  error
		url  string
	}
	ch := make(chan fetchResult, 2)
	for _, u := range []string{apiLeadrateRatesURL, apiLeadrateProposalsURL} {
		u := u
		go func() {
			b, e := httpGetJSON(ctx, u)
			ch <- fetchResult{b, e, u}
		}()
	}
	var ratesBody, propsBody []byte
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err != nil {
			return fmt.Errorf("fetch %s: %w", r.url, r.err)
		}
		if strings.Contains(r.url, "/rates") {
			ratesBody = r.body
		} else {
			propsBody = r.body
		}
	}

	// approved
	var rates apiLeadrateRatesResp
	if err := json.Unmarshal(ratesBody, &rates); err != nil {
		return fmt.Errorf("unmarshal rates: %w", err)
	}
	approved := []*store.LeadrateApproved{}
	for _, byModule := range rates.List {
		for _, rows := range byModule {
			for _, r := range rows {
				approved = append(approved, &store.LeadrateApproved{
					ChainID:      r.ChainID,
					Module:       r.Module,
					Count:        r.Count,
					Created:      r.Created,
					Blockheight:  r.Blockheight,
					TxHash:       r.TxHash,
					ApprovedRate: r.ApprovedRate,
				})
			}
		}
	}
	if err := st.BulkUpsertLeadrateApproved(approved); err != nil {
		return fmt.Errorf("store approved: %w", err)
	}

	// proposed
	var props apiLeadrateProposalsResp
	if err := json.Unmarshal(propsBody, &props); err != nil {
		return fmt.Errorf("unmarshal proposals: %w", err)
	}
	proposed := []*store.LeadrateProposed{}
	for _, byModule := range props.List {
		for _, rows := range byModule {
			for _, r := range rows {
				proposed = append(proposed, &store.LeadrateProposed{
					ChainID:     r.ChainID,
					Module:      r.Module,
					Count:       r.Count,
					Created:     r.Created,
					Blockheight: r.Blockheight,
					TxHash:      r.TxHash,
					Proposer:    r.Proposer,
					NextChange:  r.NextChange,
					NextRate:    r.NextRate,
				})
			}
		}
	}
	if err := st.BulkUpsertLeadrateProposed(proposed); err != nil {
		return fmt.Errorf("store proposed: %w", err)
	}
	return nil
}

// ---------------- ponder graphql ----------------
//
// Ponder responds with GraphQL JSON. Errors come back in `errors[]`. Pagination
// uses cursor-based `after`/`pageInfo`. The two queries we run here both fit
// well under any one-page result for FPS — there are <200 unique balance rows
// for the Equity token and far fewer active delegations — but we still loop
// the cursor to stay robust if that ever changes.

type ponderGqlReq struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type ponderPageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type ponderBalanceItem struct {
	Account string `json:"account"`
	Balance string `json:"balance"`
	Updated string `json:"updated"` // Ponder BigInt comes back as string
}

type ponderBalancesResp struct {
	Data struct {
		ERC20BalanceMappings struct {
			Items    []ponderBalanceItem `json:"items"`
			PageInfo ponderPageInfo      `json:"pageInfo"`
		} `json:"eRC20BalanceMappings"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

func fetchAndStoreFPSHolders(ctx context.Context, st *store.Store) error {
	// Ponder lets us filter by token contract directly — equity is FPS.
	// We sort desc by balance and cap at fpsHolderLimit.
	q := `query($token: String!, $limit: Int!) {
		eRC20BalanceMappings(
			where: { token: $token },
			orderBy: "balance",
			orderDirection: "desc",
			limit: $limit
		) {
			items { account balance updated }
			pageInfo { hasNextPage endCursor }
		}
	}`
	body, err := ponderQuery(ctx, q, map[string]any{
		"token": equityAddress,
		"limit": fpsHolderLimit,
	})
	if err != nil {
		return err
	}
	var resp ponderBalancesResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("graphql: %s", resp.Errors[0].Message)
	}
	out := make([]*store.FPSHolder, 0, len(resp.Data.ERC20BalanceMappings.Items))
	for _, it := range resp.Data.ERC20BalanceMappings.Items {
		// Drop dust holders — ponder includes anyone who's ever held FPS,
		// including zero-balance rows from when someone sold everything.
		if it.Balance == "" || it.Balance == "0" {
			continue
		}
		// `updated` is a BigInt in ponder (unix sec). Parse defensively;
		// 0 on failure is fine — we don't rely on it for ordering.
		updated, _ := strconv.ParseInt(it.Updated, 10, 64)
		out = append(out, &store.FPSHolder{
			Account: it.Account,
			Balance: it.Balance,
			Updated: updated,
		})
	}
	return st.BulkUpsertFPSHolders(out)
}

type ponderDelegationItem struct {
	Owner       string `json:"owner"`
	DelegatedTo string `json:"delegatedTo"`
}

type ponderDelegationsResp struct {
	Data struct {
		EquityDelegations struct {
			Items    []ponderDelegationItem `json:"items"`
			PageInfo ponderPageInfo         `json:"pageInfo"`
		} `json:"equityDelegations"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

func fetchAndStoreDelegations(ctx context.Context, st *store.Store) error {
	// Walk the cursor — delegations table is small but we paginate
	// defensively in case it grows.
	all := []*store.EquityDelegation{}
	var after string
	for {
		q := `query($after: String) {
			equityDelegations(after: $after, limit: 100) {
				items { owner delegatedTo }
				pageInfo { hasNextPage endCursor }
			}
		}`
		vars := map[string]any{}
		if after != "" {
			vars["after"] = after
		}
		body, err := ponderQuery(ctx, q, vars)
		if err != nil {
			return err
		}
		var resp ponderDelegationsResp
		if err := json.Unmarshal(body, &resp); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
		if len(resp.Errors) > 0 {
			return fmt.Errorf("graphql: %s", resp.Errors[0].Message)
		}
		for _, it := range resp.Data.EquityDelegations.Items {
			all = append(all, &store.EquityDelegation{
				Owner:       it.Owner,
				DelegatedTo: it.DelegatedTo,
			})
		}
		if !resp.Data.EquityDelegations.PageInfo.HasNextPage {
			break
		}
		after = resp.Data.EquityDelegations.PageInfo.EndCursor
		if after == "" {
			break // defensive — shouldn't happen if hasNextPage=true
		}
	}
	return st.BulkUpsertDelegations(all)
}

// ---------------- http helpers ----------------

func httpGetJSON(ctx context.Context, url string) ([]byte, error) {
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "GET", url, nil)
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
	return io.ReadAll(resp.Body)
}

func ponderQuery(ctx context.Context, query string, vars map[string]any) ([]byte, error) {
	payload, err := json.Marshal(ponderGqlReq{Query: query, Variables: vars})
	if err != nil {
		return nil, err
	}
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "POST", ponderURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
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
