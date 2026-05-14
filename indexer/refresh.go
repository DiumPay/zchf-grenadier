package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

// Refresh scans MintingUpdate + PositionDenied across all known positions
// and re-hydrates any that emitted events in [from, to].
func Refresh(
	ctx context.Context,
	ch *chain.Client,
	st *store.Store,
	collateralMeta map[string]chain.ERC20Meta,
	from, to uint64,
) (int, error) {
	logs, err := ch.GetLogs(ctx, chain.LogFilter{
		FromBlock: from,
		ToBlock:   to,
		Topics: [][]string{
			{chain.TopicMintingUpdate, chain.TopicPositionDenied},
		},
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

	// Only re-hydrate addresses we already know
	known := make(map[string]bool)
	addrs, err := st.AllAddresses()
	if err != nil {
		return 0, err
	}
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
