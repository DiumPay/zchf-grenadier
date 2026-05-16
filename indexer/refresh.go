package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/DiumPay/zchf-grenadier/chain"
)

// Refresh scans MintingUpdate + PositionDenied across all known positions
// and re-hydrates any that emitted events in [from, to].
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

	// Chunk addresses for getLogs: most public RPCs cap the address list
	// around 1024 entries (some lower). Issue parallel-by-chunk calls and
	// merge.
	const addrChunk = 500
	logs, err := getLogsChunked(ctx, ch, from, to, addrs, addrChunk, []string{
		chain.TopicMintingUpdate, chain.TopicPositionDenied,
	})
	if err != nil {
		return 0, fmt.Errorf("refresh getLogs: %w", err)
	}
	if len(logs) == 0 {
		return 0, nil
	}

	// Dedupe — same address might fire multiple events in the window
	touched := make(map[string]uint64)
	for _, log := range logs {
		addr := strings.ToLower(log.Address)
		bn := log.BlockNum()
		if bn > touched[addr] {
			touched[addr] = bn
		}
	}

	// Safety net: belt-and-suspenders in case an RPC ignores the address filter.
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
