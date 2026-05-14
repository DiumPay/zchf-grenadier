package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

// Discover scans MintingHubV2 for new PositionOpened events in [from, to]
// and hydrates each new position into the store.
func Discover(
	ctx context.Context,
	ch *chain.Client,
	st *store.Store,
	collateralMeta map[string]chain.ERC20Meta,
	from, to uint64,
) (int, error) {
	logs, err := ch.GetLogs(ctx, chain.LogFilter{
		FromBlock: from,
		ToBlock:   to,
		Addresses: []string{chain.MintingHubV2},
		Topics:    [][]string{{chain.TopicPositionOpened}},
	})
	if err != nil {
		return 0, fmt.Errorf("discover getLogs: %w", err)
	}
	if len(logs) == 0 {
		return 0, nil
	}

	count := 0
	for _, log := range logs {
		// event PositionOpened(address indexed owner, address indexed position, address original, address collateral)
		if len(log.Topics) < 3 {
			continue
		}
		emittedOwner := chain.TopicToAddress(log.Topics[1])
		positionAddr := chain.TopicToAddress(log.Topics[2])

		// CloneHelper trick: real user might be in OwnershipTransferred log
		realOwner, _ := ch.ResolveOwner(ctx, emittedOwner, positionAddr, log.TxHash)

		pos, err := ch.HydratePosition(ctx, positionAddr, collateralMeta)
		if err != nil {
			fmt.Printf("[discover] hydrate %s: %v\n", positionAddr, err)
			continue
		}

		pos.Owner = strings.ToLower(realOwner)
		if pos.Created == 0 {
			pos.Created = int64(log.BlockNum())
		}

		if err := st.Upsert(pos, int64(log.BlockNum())); err != nil {
			fmt.Printf("[discover] upsert %s: %v\n", positionAddr, err)
			continue
		}
		count++
	}
	return count, nil
}
