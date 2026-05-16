package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/DiumPay/zchf-grenadier/store"
)

const (
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

// apiMinterEntry matches the official /ecosystem/minter/list shape. Optional
// fields use *string for null vs empty distinction. When the same response
// comes from a grenadier peer (which emits "" for missing values, not null),
// the pointer still decodes correctly — empty string is just empty string.
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
	var resp apiMinterList
	if _, err := FetchAndDecodeFromPeers(ctx, EndpointMinters, 30*time.Second, &resp); err != nil {
		return err
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
//
// Two source shapes:
//
//   - Official api.frankencoin.com — two endpoints, nested:
//     /savings/leadrate/rates      → {list: {chainId: {module: [rows...]}}}
//     /savings/leadrate/proposals  → {list: {chainId: {module: [rows...]}}}
//
//   - Grenadier peer — one combined endpoint, flat:
//     /governance/leadrate → {approved: {num, list:[...]}, proposed: {num, list:[...]}}
//
// Try grenadier peer(s) first (single round-trip), fall back to official
// (two parallel round-trips). Decoded rows are normalised before storage
// so consumers see the same store types either way.

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

// Grenadier combined-response shape.
type grenadierLeadrateResp struct {
	Approved struct {
		List []*store.LeadrateApproved `json:"list"`
	} `json:"approved"`
	Proposed struct {
		List []*store.LeadrateProposed `json:"list"`
	} `json:"proposed"`
}

// Official nested response shape.
type apiLeadrateRatesResp struct {
	List map[string]map[string][]apiLeadrateRow `json:"list"`
}
type apiLeadrateProposalsResp struct {
	List map[string]map[string][]apiLeadrateRow `json:"list"`
}

func fetchAndStoreLeadrate(ctx context.Context, st *store.Store) error {
	// Try each peer in order. First successful response wins.
	var lastErr error
	for _, p := range IterPeers() {
		switch p.Kind {
		case PeerGrenadier:
			if err := fetchLeadrateFromGrenadier(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("grenadier %s: %w", p.Name, err)
				fmt.Printf("[peer] %s /governance/leadrate -> %v\n", p.Name, err)
			}
		case PeerOfficial:
			if err := fetchLeadrateFromOfficial(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("official %s: %w", p.Name, err)
				fmt.Printf("[peer] %s /savings/leadrate/* -> %v\n", p.Name, err)
			}
		case PeerPonder:
			continue // not a leadrate source
		}
	}
	if lastErr == nil {
		return fmt.Errorf("no leadrate source available")
	}
	return lastErr
}

func fetchLeadrateFromGrenadier(ctx context.Context, st *store.Store, p Peer) error {
	body, err := httpGetWithTimeout(ctx, p.Base+"/governance/leadrate", 30*time.Second)
	if err != nil {
		return err
	}
	var resp grenadierLeadrateResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if err := st.BulkUpsertLeadrateApproved(resp.Approved.List); err != nil {
		return fmt.Errorf("store approved: %w", err)
	}
	if err := st.BulkUpsertLeadrateProposed(resp.Proposed.List); err != nil {
		return fmt.Errorf("store proposed: %w", err)
	}
	fmt.Printf("[peer] %s /governance/leadrate -> ok (%d approved, %d proposed)\n",
		p.Name, len(resp.Approved.List), len(resp.Proposed.List))
	return nil
}

func fetchLeadrateFromOfficial(ctx context.Context, st *store.Store, p Peer) error {
	// Two endpoints in parallel.
	type fetchResult struct {
		body []byte
		err  error
		kind string // "rates" or "proposals"
	}
	ch := make(chan fetchResult, 2)
	go func() {
		b, e := httpGetWithTimeout(ctx, p.Base+"/savings/leadrate/rates", 30*time.Second)
		ch <- fetchResult{b, e, "rates"}
	}()
	go func() {
		b, e := httpGetWithTimeout(ctx, p.Base+"/savings/leadrate/proposals", 30*time.Second)
		ch <- fetchResult{b, e, "proposals"}
	}()
	var ratesBody, propsBody []byte
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err != nil {
			return fmt.Errorf("fetch %s: %w", r.kind, r.err)
		}
		if r.kind == "rates" {
			ratesBody = r.body
		} else {
			propsBody = r.body
		}
	}

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
	fmt.Printf("[peer] %s /savings/leadrate/* -> ok (%d approved, %d proposed)\n",
		p.Name, len(approved), len(proposed))
	return nil
}

// ---------------- FPS holders + delegations ----------------
//
// Two source shapes:
//
//   - Grenadier peer — REST GET, returns {num, list:[...]} with the same
//     store types we use internally. One round-trip per endpoint.
//
//   - Ponder GraphQL — POST with cursor pagination, different JSON shape.
//
// Try grenadier first (simpler, faster), fall back to ponder.

func fetchAndStoreFPSHolders(ctx context.Context, st *store.Store) error {
	var lastErr error
	for _, p := range IterPeers() {
		switch p.Kind {
		case PeerGrenadier:
			if err := fetchFPSHoldersFromGrenadier(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("grenadier %s: %w", p.Name, err)
				fmt.Printf("[peer] %s /governance/fps-holders -> %v\n", p.Name, err)
			}
		case PeerPonder:
			if err := fetchFPSHoldersFromPonder(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("ponder %s: %w", p.Name, err)
				fmt.Printf("[peer] %s graphql -> %v\n", p.Name, err)
			}
		case PeerOfficial:
			continue // not an FPS source
		}
	}
	if lastErr == nil {
		return fmt.Errorf("no FPS source available")
	}
	return lastErr
}

func fetchFPSHoldersFromGrenadier(ctx context.Context, st *store.Store, p Peer) error {
	body, err := httpGetWithTimeout(ctx, p.Base+"/governance/fps-holders", 30*time.Second)
	if err != nil {
		return err
	}
	var resp struct {
		List []*store.FPSHolder `json:"list"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	fmt.Printf("[peer] %s /governance/fps-holders -> ok (%d holders)\n", p.Name, len(resp.List))
	return st.BulkUpsertFPSHolders(resp.List)
}

func fetchFPSHoldersFromPonder(ctx context.Context, st *store.Store, p Peer) error {
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
	body, err := ponderQueryAt(ctx, p.Base, q, map[string]any{
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
		if it.Balance == "" || it.Balance == "0" {
			continue
		}
		updated, _ := strconv.ParseInt(it.Updated, 10, 64)
		out = append(out, &store.FPSHolder{
			Account: it.Account,
			Balance: it.Balance,
			Updated: updated,
		})
	}
	fmt.Printf("[peer] %s graphql FPS -> ok (%d holders)\n", p.Name, len(out))
	return st.BulkUpsertFPSHolders(out)
}

func fetchAndStoreDelegations(ctx context.Context, st *store.Store) error {
	var lastErr error
	for _, p := range IterPeers() {
		switch p.Kind {
		case PeerGrenadier:
			if err := fetchDelegationsFromGrenadier(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("grenadier %s: %w", p.Name, err)
				fmt.Printf("[peer] %s /governance/delegations -> %v\n", p.Name, err)
			}
		case PeerPonder:
			if err := fetchDelegationsFromPonder(ctx, st, p); err == nil {
				return nil
			} else {
				lastErr = fmt.Errorf("ponder %s: %w", p.Name, err)
				fmt.Printf("[peer] %s graphql delegations -> %v\n", p.Name, err)
			}
		case PeerOfficial:
			continue // not a delegations source
		}
	}
	if lastErr == nil {
		return fmt.Errorf("no delegations source available")
	}
	return lastErr
}

func fetchDelegationsFromGrenadier(ctx context.Context, st *store.Store, p Peer) error {
	body, err := httpGetWithTimeout(ctx, p.Base+"/governance/delegations", 30*time.Second)
	if err != nil {
		return err
	}
	var resp struct {
		List []*store.EquityDelegation `json:"list"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	fmt.Printf("[peer] %s /governance/delegations -> ok (%d edges)\n", p.Name, len(resp.List))
	return st.BulkUpsertDelegations(resp.List)
}

func fetchDelegationsFromPonder(ctx context.Context, st *store.Store, p Peer) error {
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
		body, err := ponderQueryAt(ctx, p.Base, q, vars)
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
			break
		}
	}
	fmt.Printf("[peer] %s graphql delegations -> ok (%d edges)\n", p.Name, len(all))
	return st.BulkUpsertDelegations(all)
}

// ---------------- ponder graphql wire types ----------------

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

// ponderQueryAt — POST a GraphQL request to the given ponder base URL.
// Accepts a base so callers can switch peers without state.
func ponderQueryAt(ctx context.Context, base, query string, vars map[string]any) ([]byte, error) {
	payload, err := json.Marshal(ponderGqlReq{Query: query, Variables: vars})
	if err != nil {
		return nil, err
	}
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, "POST", base, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
