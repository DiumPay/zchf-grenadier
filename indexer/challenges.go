package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

// ScanChallenges pulls ChallengeStarted/Averted/Succeeded in [from, to] in one
// log call, resolves bidders via batched eth_getBlockByNumber, and writes
// challenge + bid rows.
//
// Started:     creates a challenge row, status=Active
// Averted:     subtracts from challenge size, inserts a phase-1 bid row
// Succeeded:   marks challenge done (or partial), inserts a phase-2 bid row
//
// Logs within a block are processed in logIndex order so partial fills and
// final status flip happen in the right order.
func ScanChallenges(
	ctx context.Context,
	ch *chain.Client,
	st *store.Store,
	from, to uint64,
) (int, error) {
	logs, err := ch.GetLogs(ctx, chain.LogFilter{
		FromBlock: from,
		ToBlock:   to,
		Addresses: []string{chain.MintingHubV2},
		Topics: [][]string{{
			chain.TopicChallengeStarted,
			chain.TopicChallengeAverted,
			chain.TopicChallengeSucceed,
		}},
	})
	if err != nil {
		return 0, fmt.Errorf("challenges getLogs: %w", err)
	}
	if len(logs) == 0 {
		return 0, nil
	}

	// Group logs by block so we can do one eth_getBlockByNumber per block
	// to grab all tx senders at once. Then sort within block by logIndex
	// for correct ordering of partial fills.
	byBlock := map[uint64][]chain.Log{}
	for _, l := range logs {
		byBlock[l.BlockNum()] = append(byBlock[l.BlockNum()], l)
	}

	// Iterate blocks in ascending order for determinism.
	blockNums := make([]uint64, 0, len(byBlock))
	for bn := range byBlock {
		blockNums = append(blockNums, bn)
	}
	sort.Slice(blockNums, func(i, j int) bool { return blockNums[i] < blockNums[j] })

	count := 0
	for _, bn := range blockNums {
		blockLogs := byBlock[bn]
		sort.Slice(blockLogs, func(i, j int) bool {
			return blockLogs[i].LogIdx() < blockLogs[j].LogIdx()
		})

		// One eth_getBlockByNumber per block gives us both:
		//   - tx.from for every tx (bidder resolution)
		//   - block timestamp (challenge start time)
		// We always fetch even if only Started events are present, because
		// Started needs the timestamp for phase-window computations.
		block, err := ch.GetBlock(ctx, bn)
		if err != nil {
			fmt.Printf("[challenges] get block %d: %v\n", bn, err)
			continue
		}
		blockTs := block.Ts()
		senders := make(map[string]string, len(block.Transactions))
		for _, tx := range block.Transactions {
			senders[strings.ToLower(tx.Hash)] = strings.ToLower(tx.From)
		}

		for _, log := range blockLogs {
			switch log.Topics[0] {
			case chain.TopicChallengeStarted:
				if err := handleStarted(ctx, ch, st, log, blockTs); err != nil {
					fmt.Printf("[challenges] started %s: %v\n", log.TxHash, err)
					continue
				}
			case chain.TopicChallengeAverted:
				bidder := senders[strings.ToLower(log.TxHash)]
				if err := handleAverted(st, log, bidder); err != nil {
					fmt.Printf("[challenges] averted %s: %v\n", log.TxHash, err)
					continue
				}
			case chain.TopicChallengeSucceed:
				bidder := senders[strings.ToLower(log.TxHash)]
				if err := handleSucceeded(st, log, bidder); err != nil {
					fmt.Printf("[challenges] succeeded %s: %v\n", log.TxHash, err)
					continue
				}
			default:
				continue
			}
			count++
		}
	}
	return count, nil
}

// event ChallengeStarted(address indexed challenger, address indexed position, uint256 size, uint256 number)
//
//	topics[1] = challenger
//	topics[2] = position
//	data      = size (32) || number (32)
func handleStarted(ctx context.Context, ch *chain.Client, st *store.Store, log chain.Log, blockTs int64) error {
	if len(log.Topics) < 3 {
		return fmt.Errorf("Started: missing topics")
	}
	challenger := chain.TopicToAddress(log.Topics[1])
	position := chain.TopicToAddress(log.Topics[2])

	data, err := hexToBytes(log.Data)
	if err != nil || len(data) < 64 {
		return fmt.Errorf("Started: bad data")
	}
	size := new(big.Int).SetBytes(data[0:32])
	number := new(big.Int).SetBytes(data[32:64]).Uint64()

	// Read the position's challengePeriod + liqPrice. Best-effort: if the
	// position isn't hydrated yet we leave both at 0 and a later refresh
	// fills them in.
	var duration int64
	var liqPrice string
	if pos, _ := ch.HydratePosition(ctx, position, nil); pos != nil {
		duration = pos.ChallengePeriod
		liqPrice = pos.Price
	}

	c := &store.Challenge{
		ID:                 challengeID(position, number),
		Position:           position,
		Number:             number,
		TxHash:             log.TxHash,
		Challenger:         challenger,
		Start:              blockTs,
		Created:            blockTs,
		Duration:           duration,
		Size:               size.String(),
		LiqPrice:           liqPrice,
		Bids:               0,
		FilledSize:         "0",
		AcquiredCollateral: "0",
		Status:             "Active",
		Version:            2,
		Block:              int64(log.BlockNum()),
	}
	return st.UpsertChallenge(c, int64(log.BlockNum()))
}

// event ChallengeAverted(address indexed position, uint256 number, uint256 size)
//
//	topics[1] = position
//	data      = number (32) || size (32)
func handleAverted(st *store.Store, log chain.Log, bidder string) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("Averted: missing topics")
	}
	position := chain.TopicToAddress(log.Topics[1])
	data, err := hexToBytes(log.Data)
	if err != nil || len(data) < 64 {
		return fmt.Errorf("Averted: bad data")
	}
	number := new(big.Int).SetBytes(data[0:32]).Uint64()
	size := new(big.Int).SetBytes(data[32:64])

	id := challengeID(position, number)
	cur, err := st.GetChallenge(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("Averted on unknown challenge %s", id)
	}

	// Add size to filled, flip status if fully filled.
	filled := new(big.Int)
	filled.SetString(cur.FilledSize, 10)
	filled.Add(filled, size)
	cur.FilledSize = filled.String()
	cur.Bids++

	original := new(big.Int)
	original.SetString(cur.Size, 10)
	if filled.Cmp(original) >= 0 {
		cur.Status = "Averted"
	}

	if err := st.UpsertChallenge(cur, int64(log.BlockNum())); err != nil {
		return err
	}

	// Bid amount in ZCHF for Averted = size * liqPrice / 10^36. We have
	// liqPrice on the challenge row from handleStarted, so compute it here.
	bidAmt := "0"
	if cur.LiqPrice != "" && cur.LiqPrice != "0" {
		lp := new(big.Int)
		if _, ok := lp.SetString(cur.LiqPrice, 10); ok {
			b := new(big.Int).Mul(size, lp)
			b.Quo(b, big.NewInt(0).Exp(big.NewInt(10), big.NewInt(18), nil))
			bidAmt = b.String()
		}
	}

	numberBid := uint64(cur.Bids - 1) // 0-indexed bid within this challenge
	b := &store.Bid{
		ID:                 bidIDApi(position, number, numberBid),
		Position:           position,
		Number:             number,
		NumberBid:          numberBid,
		TxHash:             log.TxHash,
		Bidder:             bidder,
		Created:            0, // filled in by caller if available
		BidType:            "Averted",
		Bid:                bidAmt,
		Price:              cur.LiqPrice, // phase-1 price is the liq price
		FilledSize:         size.String(),
		AcquiredCollateral: "0", // averted: collateral stays with position
		ChallengeSize:      cur.Size,
		Version:            2,
		Block:              int64(log.BlockNum()),
	}
	return st.UpsertBid(b)
}

// event ChallengeSucceeded(address indexed position, uint256 number, uint256 bid, uint256 acquiredCollateral, uint256 challengeSize)
//
//	topics[1] = position
//	data      = number (32) || bid (32) || acquiredCollateral (32) || challengeSize (32)
func handleSucceeded(st *store.Store, log chain.Log, bidder string) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("Succeeded: missing topics")
	}
	position := chain.TopicToAddress(log.Topics[1])
	data, err := hexToBytes(log.Data)
	if err != nil || len(data) < 128 {
		return fmt.Errorf("Succeeded: bad data")
	}
	number := new(big.Int).SetBytes(data[0:32]).Uint64()
	bidAmt := new(big.Int).SetBytes(data[32:64])
	acquired := new(big.Int).SetBytes(data[64:96])
	chalSize := new(big.Int).SetBytes(data[96:128])

	id := challengeID(position, number)
	cur, err := st.GetChallenge(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("Succeeded on unknown challenge %s", id)
	}

	// Add acquired collateral to running total. Filled grows by chalSize
	// (what got consumed from the challenge in this bid).
	acqTotal := new(big.Int)
	acqTotal.SetString(cur.AcquiredCollateral, 10)
	acqTotal.Add(acqTotal, acquired)
	cur.AcquiredCollateral = acqTotal.String()

	filled := new(big.Int)
	filled.SetString(cur.FilledSize, 10)
	filled.Add(filled, chalSize)
	cur.FilledSize = filled.String()
	cur.Bids++

	original := new(big.Int)
	original.SetString(cur.Size, 10)
	if filled.Cmp(original) >= 0 {
		// Match the upstream API's string ("Success", not "Succeeded") so
		// downstream consumers see consistent values whether the row came
		// from bootstrap or live indexing.
		cur.Status = "Success"
	}

	if err := st.UpsertChallenge(cur, int64(log.BlockNum())); err != nil {
		return err
	}

	// Per-bid price = bid / acquiredCollateral, scaled. Frontend can recompute,
	// but storing it makes /bids responses self-contained.
	price := "0"
	if acquired.Sign() > 0 {
		// price * acquired / 1e18 = bid → price = bid * 1e18 / acquired
		e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		p := new(big.Int).Mul(bidAmt, e18)
		p.Quo(p, acquired)
		price = p.String()
	}

	numberBid := uint64(cur.Bids - 1)
	b := &store.Bid{
		ID:                 bidIDApi(position, number, numberBid),
		Position:           position,
		Number:             number,
		NumberBid:          numberBid,
		TxHash:             log.TxHash,
		Bidder:             bidder,
		Created:            0,
		BidType:            "Succeeded",
		Bid:                bidAmt.String(),
		Price:              price,
		FilledSize:         acquired.String(),
		AcquiredCollateral: acquired.String(),
		ChallengeSize:      chalSize.String(),
		Version:            2,
		Block:              int64(log.BlockNum()),
	}
	return st.UpsertBid(b)
}

// ---- helpers ----

func challengeID(position string, number uint64) string {
	// Match upstream API format exactly: "<pos>-challenge-<number>"
	return fmt.Sprintf("%s-challenge-%d", strings.ToLower(position), number)
}

func bidIDApi(position string, challengeNum, bidNum uint64) string {
	// Match upstream API format: "<pos>-challenge-<n>-bid-<k>"
	return fmt.Sprintf("%s-challenge-%d-bid-%d", strings.ToLower(position), challengeNum, bidNum)
}

func hexToBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s)%2 != 0 {
		s = "0" + s
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		hi := hexVal(s[2*i])
		lo := hexVal(s[2*i+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("bad hex")
		}
		b[i] = byte(hi<<4 | lo)
	}
	return b, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}
