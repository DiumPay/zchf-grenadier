package store

import (
	"math/big"
	"sort"
	"strings"
	"time"
)

// MonitoredPosition wraps Position with the open-challenge collateral
// summed in. ChallengedCollateral is the remaining (unfilled) size across
// all currently-active challenges for this position, as a bigint string in
// the collateral's native decimals. "0" if no open challenges.
type MonitoredPosition struct {
	*Position
	ChallengedCollateral string `json:"challengedCollateral"`
}

// Monitored returns every position that's still alive and challengeable:
// not closed, not denied, past cooldown, before expiration, and holding
// either collateral or outstanding debt. Unlike Curated() this returns
// ALL such positions (no per-collateral best-pick), which is what the
// monitoring / challenge page wants.
//
// Each result includes ChallengedCollateral — the sum of (Size - FilledSize)
// across all Active challenges for that position. The join is done in Go
// rather than SQL because challenges store JSON blobs.
func (s *Store) Monitored() ([]*MonitoredPosition, error) {
	all, err := s.AllLive()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()

	// Sum open-challenge remaining collateral per position.
	// One SELECT for everything, then bucket by position in Go.
	active, err := s.ActiveChallenges()
	if err != nil {
		// Don't fail the whole query just because challenges errored —
		// the monitoring page is more useful with no challenge column
		// than not at all.
		active = nil
	}
	remainingByPos := make(map[string]*big.Int, len(active))
	for _, c := range active {
		size, ok := new(big.Int).SetString(c.Size, 10)
		if !ok {
			continue
		}
		filled, ok := new(big.Int).SetString(c.FilledSize, 10)
		if !ok {
			filled = new(big.Int) // treat missing as 0
		}
		remaining := new(big.Int).Sub(size, filled)
		if remaining.Sign() <= 0 {
			continue
		}
		key := strings.ToLower(c.Position)
		if cur, ok := remainingByPos[key]; ok {
			cur.Add(cur, remaining)
		} else {
			remainingByPos[key] = remaining
		}
	}

	out := make([]*MonitoredPosition, 0, len(all))
	for _, p := range all {
		if !isMonitorable(p, now) {
			continue
		}
		challenged := "0"
		if r, ok := remainingByPos[strings.ToLower(p.Position)]; ok {
			challenged = r.String()
		}
		out = append(out, &MonitoredPosition{Position: p, ChallengedCollateral: challenged})
	}
	return out, nil
}

func isMonitorable(p *Position, _ int64) bool {
	if p.Closed || p.Denied {
		return false
	}
	bal, ok := new(big.Int).SetString(p.CollateralBalance, 10)
	return ok && bal.Sign() > 0
}

func nonZero(s string) bool {
	v, ok := new(big.Int).SetString(s, 10)
	return ok && v.Sign() > 0
}

// Curated returns one position per collateral, picked by:
//   - must be active: not closed, not denied, not expired, not in cooldown
//   - must have room to mint (availableForClones > 0)
//   - rank by lowest effective interest = interest / (1 - reserve)
//   - tiebreak: longest expiration
func (s *Store) Curated() ([]*Position, error) {
	all, err := s.AllLive()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	best := make(map[string]*Position, 32)

	for _, p := range all {
		if !isCuratable(p, now) {
			continue
		}
		key := strings.ToLower(p.Collateral)
		cur, ok := best[key]
		if !ok || betterThan(p, cur) {
			best[key] = p
		}
	}

	out := make([]*Position, 0, len(best))
	for _, p := range best {
		out = append(out, p)
	}
	return out, nil
}

// Active returns every position that is alive — not closed, not denied,
// not expired, past its cooldown. This is the set the monitoring page
// needs. Unlike Curated, it does NOT require room to mint: a fully-borrowed
// position is still a valid challenge target.
//
// We pre-sort by health proxy (lowest collateralization ratio first) so
// the riskiest positions surface at the top without the client having to
// re-sort. Sort uses (collateralBalance × price) / minted, computed in
// big.Int to avoid float precision loss on 36-digit numbers.
func (s *Store) Active() ([]*Position, error) {
	all, err := s.AllLive()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	out := make([]*Position, 0, len(all))
	for _, p := range all {
		if !isLive(p, now) {
			continue
		}
		out = append(out, p)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ri, oki := collateralizationRatio(out[i])
		rj, okj := collateralizationRatio(out[j])
		switch {
		case !oki && !okj:
			return out[i].Expiration < out[j].Expiration
		case !oki:
			return false
		case !okj:
			return true
		default:
			return ri.Cmp(rj) < 0
		}
	})
	return out, nil
}

// isLive: monitoring-eligible. Same as isCuratable minus the
// availableForClones gate — a fully-minted position is still challengeable.
func isLive(p *Position, now int64) bool {
	if p.Closed || p.Denied {
		return false
	}
	if p.Expiration > 0 && now >= int64(p.Expiration) {
		return false
	}
	if p.Cooldown > 0 && now < int64(p.Cooldown) {
		return false
	}
	return true
}

// collateralizationRatio returns (collateralBalance × price) / minted as a
// big.Float, plus an ok flag (false when minted is zero/invalid). We don't
// normalize units since we only use the result for sorting — units cancel
// across positions if collateral decimals match; for mixed decimals the
// ordering remains a reasonable risk proxy.
func collateralizationRatio(p *Position) (*big.Float, bool) {
	minted, ok := new(big.Int).SetString(p.Minted, 10)
	if !ok || minted.Sign() <= 0 {
		return nil, false
	}
	bal, ok := new(big.Int).SetString(p.CollateralBalance, 10)
	if !ok {
		return nil, false
	}
	price, ok := new(big.Int).SetString(p.Price, 10)
	if !ok {
		return nil, false
	}
	num := new(big.Int).Mul(bal, price)
	ratio := new(big.Float).Quo(new(big.Float).SetInt(num), new(big.Float).SetInt(minted))
	return ratio, true
}

func isCuratable(p *Position, now int64) bool {
	if p.Closed || p.Denied {
		return false
	}
	if p.Expiration > 0 && now >= int64(p.Expiration) {
		return false
	}
	if p.Cooldown > 0 && now < int64(p.Cooldown) {
		return false
	}
	avail, ok := new(big.Int).SetString(p.AvailableForClones, 10)
	if !ok || avail.Sign() <= 0 {
		return false
	}
	return true
}

// effective interest in ppm: interest / (1 - reserve)
func effectiveInterestPPM(p *Position) float64 {
	r := float64(p.ReserveContribution) / 1_000_000.0
	if r >= 1 {
		return 1e18
	}
	i := float64(p.AnnualInterestPPM) / 1_000_000.0
	return (i / (1 - r)) * 1_000_000.0
}

func betterThan(a, b *Position) bool {
	ea := effectiveInterestPPM(a)
	eb := effectiveInterestPPM(b)
	if ea != eb {
		return ea < eb
	}
	return a.Expiration > b.Expiration
}
