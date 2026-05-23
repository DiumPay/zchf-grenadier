package indexer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

const (
	tickInterval  = 12 * time.Second
	maxBlockRange = 2000 // safe for almost every public RPC
	safetyDepth   = 5    // don't index up to head — reorg margin
	// overlap: re-scan last N blocks each tick. Tuned wide enough that a
	// single lying RPC (one that returns [] for eth_getLogs without erroring)
	// won't permanently skip events — by the next tick the same range is
	// still in the re-scan window and gets another shot at a different
	// racer. ~300 blocks ≈ 60 min on mainnet. getLogs on a tiny address set
	// is essentially free; the cost of being too generous here is nothing.
	overlap = 300
)

type Indexer struct {
	ch *chain.Client
	st *store.Store

	collateralMeta map[string]chain.ERC20Meta

	// Known position-address cache. Refresh + ScanChallenges read this every
	// tick; rebuilding from SQL each time was a needless full table scan.
	// Discover bumps `knownDirty` after a successful insert; the next reader
	// reloads. Safe under writeMu serialization at the store level.
	knownMu    sync.RWMutex
	known      []string
	knownDirty bool
}

func New(ch *chain.Client, st *store.Store) *Indexer {
	return &Indexer{
		ch:             ch,
		st:             st,
		collateralMeta: make(map[string]chain.ERC20Meta),
		knownDirty:     true, // force first-tick load
	}
}

// KnownAddresses returns the cached position-address set, reloading from the
// store iff dirty. Refresh and ScanChallenges call this instead of hitting
// AllAddresses() directly. Returned slice is read-only to callers.
func (ix *Indexer) KnownAddresses() ([]string, error) {
	ix.knownMu.RLock()
	if !ix.knownDirty && ix.known != nil {
		out := ix.known
		ix.knownMu.RUnlock()
		return out, nil
	}
	ix.knownMu.RUnlock()

	ix.knownMu.Lock()
	defer ix.knownMu.Unlock()
	// Double-check after upgrading lock.
	if !ix.knownDirty && ix.known != nil {
		return ix.known, nil
	}
	addrs, err := ix.st.AllAddresses()
	if err != nil {
		return nil, err
	}
	ix.known = addrs
	ix.knownDirty = false
	return ix.known, nil
}

// MarkKnownDirty: called after Discover inserts a new position.
func (ix *Indexer) MarkKnownDirty() {
	ix.knownMu.Lock()
	ix.knownDirty = true
	ix.knownMu.Unlock()
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

	var newCount, refreshCount, chalCount int
	for from <= target {
		// Honor cancellation between chunks so SIGTERM doesn't have to wait
		// out a full chunk's worth of multicalls.
		select {
		case <-ctx.Done():
			return
		default:
		}
		to := from + maxBlockRange
		if to > target {
			to = target
		}

		n1, err := Discover(ctx, ix, from, to)
		if err != nil {
			fmt.Printf("[tick] discover [%d,%d]: %v\n", from, to, err)
			return
		}
		newCount += n1

		n2, err := Refresh(ctx, ix, from, to)
		if err != nil {
			fmt.Printf("[tick] refresh [%d,%d]: %v\n", from, to, err)
			return
		}
		refreshCount += n2

		n3, err := ScanChallenges(ctx, ix.ch, ix.st, from, to)
		if err != nil {
			fmt.Printf("[tick] challenges [%d,%d]: %v\n", from, to, err)
			return
		}
		chalCount += n3

		from = to + 1
	}

	if err := ix.st.SetLastBlock(target); err != nil {
		fmt.Printf("[tick] set last block: %v\n", err)
		return
	}

	if newCount > 0 || refreshCount > 0 || chalCount > 0 {
		fmt.Printf("[tick] %d new, %d refreshed, %d challenge events, block %d, took %v\n",
			newCount, refreshCount, chalCount, target, time.Since(t0))
	}
}

// LastBlock returns the most recently scanned block (for /health).
func (ix *Indexer) LastBlock() uint64 {
	n, _ := ix.st.GetLastBlock()
	return n
}
