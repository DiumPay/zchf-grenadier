package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/DiumPay/zchf-grenadier/chain"
)

// Refresh scans every event that changes the on-chain state of a known
// position and re-hydrates the affected positions.
//
// Topics watched:
//   - MintingUpdate:   fires on every mint, repay, collateral add/withdraw
//     done via the position contract itself
//   - PositionDenied:  governance veto closes the position
//   - ForcedSale:      MintingHubV2 sells off collateral of expired
//     positions WITHOUT emitting MintingUpdate; without
//     this topic the stored CollateralBalance drifts
//     away from on-chain truth and stale positions
//     linger forever in /positions/monitored
func Refresh(
	ctx context.Context,
	ix *Indexer,
	from, to uint64,
) (int, error) {
	ch, st, collateralMeta := ix.ch, ix.st, ix.collateralMeta

	// Scope getLogs by the known position address set so the RPC returns only
	// events from contracts we care about. Previously this pulled every
	// MintingUpdate on mainnet and discarded ~all of them client-side.
	addrs, err := ix.KnownAddresses()
	if err != nil {
		return 0, fmt.Errorf("refresh: load addrs: %w", err)
	}
	if len(addrs) == 0 {
		return 0, nil
	}

	// ForcedSale is emitted by MintingHubV2, not by the position contract,
	// and identifies the affected position in its non-indexed data field
	// (not in topics). We can't address-filter for it the same way as the
	// per-position events, so we union the hub into the address set for
	// this one getLogs call. The hub address is small so this adds one
	// entry; the touched-set logic below filters down to known positions.
	addrsWithHub := append(addrs, chain.MintingHubV2)

	// Chunk addresses for getLogs: most public RPCs cap the address list
	// around 1024 entries (some lower). Issue parallel-by-chunk calls and
	// merge.
	const addrChunk = 500
	logs, err := getLogsChunked(ctx, ch, from, to, addrsWithHub, addrChunk, []string{
		chain.TopicMintingUpdate,
		chain.TopicPositionDenied,
		chain.TopicForcedSale,
	})
	if err != nil {
		return 0, fmt.Errorf("refresh getLogs: %w", err)
	}
	if len(logs) == 0 {
		return 0, nil
	}

	// Dedupe — same address might fire multiple events in the window.
	// For ForcedSale (emitted by the hub) we extract the position address
	// from the first 32 bytes of the log data; for the per-position events
	// we use log.Address directly.
	touched := make(map[string]uint64)
	for _, log := range logs {
		var target string
		if len(log.Topics) > 0 && strings.EqualFold(log.Topics[0], chain.TopicForcedSale) {
			target = extractFirstAddress(log.Data)
			if target == "" {
				continue
			}
		} else {
			target = strings.ToLower(log.Address)
		}
		bn := log.BlockNum()
		if bn > touched[target] {
			touched[target] = bn
		}
	}

	// Safety net: belt-and-suspenders in case an RPC ignores the address
	// filter, plus this filters out the hub itself (we only re-hydrate
	// positions, never the hub).
	known := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		known[a] = true
	}

	count := 0
	for addr, blockNum := range touched {
		if !known[addr] {
			continue
		}
		pos, err := ch.HydratePosition(ctx, addr, collateralMeta)
		if err != nil {
			fmt.Printf("[refresh] hydrate %s: %v\n", addr, err)
			continue
		}
		if err := st.Upsert(pos, int64(blockNum)); err != nil {
			fmt.Printf("[refresh] upsert %s: %v\n", addr, err)
			continue
		}
		count++
	}
	return count, nil
}

// extractFirstAddress pulls a left-padded address out of the first 32-byte
// word of an eth_getLogs `data` field. Used for ForcedSale where the
// affected position is the first non-indexed parameter:
//
//	event ForcedSale(address pos, uint256 amount, uint256 priceE36MinusDecimals);
//
// Returns "" on malformed input (caller skips).
func extractFirstAddress(data string) string {
	data = strings.TrimPrefix(data, "0x")
	if len(data) < 64 {
		return ""
	}
	return "0x" + strings.ToLower(data[24:64])
}
