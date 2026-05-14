package store

import (
	"math/big"
	"strings"
	"time"
)

// Curated returns one position per collateral, picked by:
//   - must be active: not closed, not denied, not expired, not in cooldown
//   - must have room to mint (availableForClones > 0)
//   - rank by lowest effective interest = interest / (1 - reserve)
//   - tiebreak: longest expiration
func (s *Store) Curated() ([]*Position, error) {
	all, err := s.All()
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
