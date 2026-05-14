package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

const (
	tickInterval  = 12 * time.Second
	maxBlockRange = 2000 // safe for almost every public RPC
	safetyDepth   = 5    // don't index up to head — reorg margin
	overlap       = 10   // re-scan last N blocks each tick for extra safety
)

type Indexer struct {
	ch *chain.Client
	st *store.Store

	collateralMeta map[string]chain.ERC20Meta
}

func New(ch *chain.Client, st *store.Store) *Indexer {
	return &Indexer{
		ch:             ch,
		st:             st,
		collateralMeta: make(map[string]chain.ERC20Meta),
	}
}

// Run starts the tick loop. Blocks until ctx is cancelled.
func (ix *Indexer) Run(ctx context.Context) {
	ix.tick(ctx) // immediate first tick

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ix.tick(ctx)
		}
	}
}

func (ix *Indexer) tick(ctx context.Context) {
	t0 := time.Now()

	head, err := ix.ch.BlockNumber(ctx)
	if err != nil {
		fmt.Printf("[tick] block: %v\n", err)
		return
	}
	target := head
	if target > safetyDepth {
		target -= safetyDepth
	}

	last, err := ix.st.GetLastBlock()
	if err != nil {
		fmt.Printf("[tick] last block: %v\n", err)
		return
	}
	if last == 0 {
		// no bootstrap — start fresh from target
		last = target
		if last > 0 {
			last--
		}
	}
	if last >= target {
		return // nothing new
	}

	from := last + 1
	if from > overlap {
		from -= overlap // re-scan recent window for reorg safety
	}

	var newCount, refreshCount int
	for from <= target {
		to := from + maxBlockRange
		if to > target {
			to = target
		}

		n1, err := Discover(ctx, ix.ch, ix.st, ix.collateralMeta, from, to)
		if err != nil {
			fmt.Printf("[tick] discover [%d,%d]: %v\n", from, to, err)
			return
		}
		newCount += n1

		n2, err := Refresh(ctx, ix.ch, ix.st, ix.collateralMeta, from, to)
		if err != nil {
			fmt.Printf("[tick] refresh [%d,%d]: %v\n", from, to, err)
			return
		}
		refreshCount += n2

		from = to + 1
	}

	if err := ix.st.SetLastBlock(target); err != nil {
		fmt.Printf("[tick] set last block: %v\n", err)
		return
	}

	if newCount > 0 || refreshCount > 0 {
		fmt.Printf("[tick] %d new, %d refreshed, block %d, took %v\n",
			newCount, refreshCount, target, time.Since(t0))
	}
}

// LastBlock returns the most recently scanned block (for /health).
func (ix *Indexer) LastBlock() uint64 {
	n, _ := ix.st.GetLastBlock()
	return n
}
