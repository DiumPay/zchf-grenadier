package chain

import (
	"context"
	"fmt"
	"strings"

	"github.com/DiumPay/zchf-grenadier/store"
)

var (
	MintingHubV2 = strings.ToLower("0xDe12B620A8a714476A97EfD14E6F7180Ca653557")
	CloneHelper  = strings.ToLower("0x55cD2820735Db56ca0965BE224D71994265F8bee")
)

type ERC20Meta struct {
	Name     string
	Symbol   string
	Decimals int
}

// HydratePosition reads all the fields of a position in one multicall.
func (c *Client) HydratePosition(
	ctx context.Context,
	addr string,
	collateralMeta map[string]ERC20Meta,
) (*store.Position, error) {
	addr = strings.ToLower(addr)

	// Build the 18 calls
	sels := []struct {
		name string
		sel  [4]byte
	}{
		{"owner", selOwner},
		{"original", selOriginal},
		{"collateral", selCollateral},
		{"zchf", selZchf},
		{"price", selPrice},
		{"minted", selMinted},
		{"minimumCollateral", selMinimumCollateral},
		{"limit", selLimit},
		{"availableForClones", selAvailableForClones},
		{"availableForMinting", selAvailableForMinting},
		{"start", selStart},
		{"expiration", selExpiration},
		{"cooldown", selCooldown},
		{"challengePeriod", selChallengePeriod},
		{"riskPremiumPPM", selRiskPremiumPPM},
		{"reserveContribution", selReserveContribution},
		{"isClosed", selIsClosed},
		{"challengedAmount", selChallengedAmount},
	}

	calls := make([]mcCall, len(sels))
	for i, s := range sels {
		calls[i] = mcCall{target: addr, allowFailure: true, callData: s.sel[:]}
	}

	data, err := encodeAggregate3(calls)
	if err != nil {
		return nil, err
	}
	resp, err := c.ethCall(ctx, Multicall3Address, data)
	if err != nil {
		return nil, fmt.Errorf("hydrate %s: %w", addr, err)
	}
	results, err := decodeAggregate3Result(resp)
	if err != nil {
		return nil, fmt.Errorf("hydrate %s decode: %w", addr, err)
	}

	r := func(i int) []byte {
		if i >= len(results) || !results[i].Success {
			return nil
		}
		return results[i].ReturnData
	}

	mustAddr := func(b []byte) string { a, _ := decodeAddress(b, 0); return a }
	mustBig := func(b []byte) string {
		v, _ := decodeUint256(b, 0)
		if v == nil {
			return "0"
		}
		return v.String()
	}
	mustU64 := func(b []byte) int64 { v, _ := decodeUint64(b, 0); return int64(v) }
	mustU32 := func(b []byte) int { v, _ := decodeUint64(b, 0); return int(v) }
	mustBool := func(b []byte) bool { v, _ := decodeBool(b, 0); return v }

	p := &store.Position{
		Version:             2,
		Position:            addr,
		Owner:               mustAddr(r(0)),
		Original:            mustAddr(r(1)),
		Collateral:          mustAddr(r(2)),
		Zchf:                mustAddr(r(3)),
		Price:               mustBig(r(4)),
		Minted:              mustBig(r(5)),
		MinimumCollateral:   mustBig(r(6)),
		LimitForPosition:    mustBig(r(7)),
		AvailableForClones:  mustBig(r(8)),
		AvailableForMinting: mustBig(r(9)),
		Start:               mustU64(r(10)),
		Expiration:          mustU64(r(11)),
		Cooldown:            mustU64(r(12)),
		ChallengePeriod:     mustU64(r(13)),
		RiskPremiumPPM:      mustU32(r(14)),
		ReserveContribution: mustU32(r(15)),
		Closed:              mustBool(r(16)),
	}
	p.IsOriginal = p.Original == p.Position
	p.IsClone = !p.IsOriginal

	if collateralMeta != nil {
		meta, ok := collateralMeta[p.Collateral]
		if !ok {
			m, err := c.HydrateERC20(ctx, p.Collateral)
			if err == nil {
				meta = *m
				collateralMeta[p.Collateral] = meta
			}
		}
		p.CollateralName = meta.Name
		p.CollateralSymbol = meta.Symbol
		p.CollateralDecimals = meta.Decimals
	}
	return p, nil
}

func (c *Client) HydrateERC20(ctx context.Context, addr string) (*ERC20Meta, error) {
	addr = strings.ToLower(addr)
	calls := []mcCall{
		{target: addr, allowFailure: true, callData: selName[:]},
		{target: addr, allowFailure: true, callData: selSymbol[:]},
		{target: addr, allowFailure: true, callData: selDecimals[:]},
	}
	data, _ := encodeAggregate3(calls)
	resp, err := c.ethCall(ctx, Multicall3Address, data)
	if err != nil {
		return nil, err
	}
	results, err := decodeAggregate3Result(resp)
	if err != nil {
		return nil, err
	}
	meta := &ERC20Meta{}
	if len(results) >= 1 && results[0].Success {
		meta.Name, _ = decodeString(results[0].ReturnData, 0)
	}
	if len(results) >= 2 && results[1].Success {
		meta.Symbol, _ = decodeString(results[1].ReturnData, 0)
	}
	if len(results) >= 3 && results[2].Success {
		d, _ := decodeUint64(results[2].ReturnData, 0)
		meta.Decimals = int(d)
	}
	return meta, nil
}

// ResolveOwner — the CloneHelper trick
func (c *Client) ResolveOwner(ctx context.Context, emittedOwner, position, txHash string) (string, error) {
	emittedOwner = strings.ToLower(emittedOwner)
	if emittedOwner != CloneHelper {
		return emittedOwner, nil
	}
	receipt, err := c.GetTransactionReceipt(ctx, txHash)
	if err != nil {
		return emittedOwner, nil
	}
	positionLower := strings.ToLower(position)
	for _, log := range receipt.Logs {
		if strings.ToLower(log.Address) != positionLower {
			continue
		}
		if len(log.Topics) < 3 || strings.ToLower(log.Topics[0]) != TopicOwnership {
			continue
		}
		if TopicToAddress(log.Topics[1]) == CloneHelper {
			return TopicToAddress(log.Topics[2]), nil
		}
	}
	return emittedOwner, nil
}
