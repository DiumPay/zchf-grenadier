package indexer

import (
	"context"
	"sync"

	"github.com/DiumPay/zchf-grenadier/chain"
)

// getLogsChunked issues eth_getLogs in parallel chunks when the address set
// exceeds what a single RPC accepts. Most public RPCs cap the address list
// per call (typically 1000–1024, some lower). This function:
//
//   - splits addrs into fixed-size chunks (chunkSize),
//   - runs each chunk concurrently through the balancer (its own pool/race
//     logic handles per-chunk fan-out),
//   - returns the merged log slice on full success,
//   - aborts on first error (caller's tick will retry next iteration).
//
// Concurrency is bounded by chunk count, not by address count — which is
// what we want, since each chunk costs one RPC round-trip and the balancer
// already races across endpoints internally.
func getLogsChunked(
	ctx context.Context,
	ch *chain.Client,
	from, to uint64,
	addrs []string,
	chunkSize int,
	topics []string,
) ([]chain.Log, error) {
	if chunkSize <= 0 {
		chunkSize = 500
	}
	if len(addrs) <= chunkSize {
		return ch.GetLogs(ctx, chain.LogFilter{
			FromBlock: from,
			ToBlock:   to,
			Addresses: addrs,
			Topics:    [][]string{topics},
		})
	}

	// Number of chunks.
	chunks := (len(addrs) + chunkSize - 1) / chunkSize
	results := make([][]chain.Log, chunks)
	errs := make([]error, chunks)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(chunks)
	for i := 0; i < chunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(addrs) {
			end = len(addrs)
		}
		go func(idx int, slice []string) {
			defer wg.Done()
			logs, err := ch.GetLogs(subCtx, chain.LogFilter{
				FromBlock: from,
				ToBlock:   to,
				Addresses: slice,
				Topics:    [][]string{topics},
			})
			if err != nil {
				errs[idx] = err
				cancel() // short-circuit siblings
				return
			}
			results[idx] = logs
		}(i, addrs[start:end])
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	// Merge. Order doesn't matter — caller dedupes by (address, max block).
	total := 0
	for _, r := range results {
		total += len(r)
	}
	out := make([]chain.Log, 0, total)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
}
