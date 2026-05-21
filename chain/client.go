package chain

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/DiumPay/zchf-grenadier/transport"
)

const Multicall3Address = "0xcA11bde05977b3631167028862bE2a173976CA11"

// Event topic hashes for Frankencoin MintingHubV2 and PositionV2.
//
// These are keccak256(eventSignature) and MUST match the deployed contracts
// exactly — a single wrong character silently breaks indexing because
// eth_getLogs filters by topic[0] and returns nothing on a mismatch.
//
// Verified against the official contract source in @frankencoin/zchf
// (contracts/minting/v2/MintingHubV2.sol and PositionV2.sol). The
// TestTopicHashes unit test re-derives all of these from the signature
// strings on every test run, so a regression cannot ship unnoticed.
const (
	// MintingHubV2.sol
	// event PositionOpened(address indexed owner, address indexed position, address original, address collateral);
	TopicPositionOpened = "0xc9b570ab9d98bdf3e38a40fd71b20edafca42449f23ca51f0bdcbf40e8ffe175"
	// event ChallengeStarted(address indexed challenger, address indexed position, uint256 size, uint256 number);
	TopicChallengeStarted = "0xc4b384b2c5ca32c8e77081f4083be594a1ea9ba34f208a9f9a458f70608585f5"
	// event ChallengeAverted(address indexed position, uint256 number, uint256 size);
	TopicChallengeAverted = "0x1eee30d91b773ac47d7485a3acb6bcd8c7c9cd8d95301b1af361baf5f0991d2e"
	// event ChallengeSucceeded(address indexed position, uint256 number, uint256 bid, uint256 acquiredCollateral, uint256 challengeSize);
	TopicChallengeSucceed = "0x7d3a26e8d43c5b70f86266bfa26c212e3c097716ff7240ccb6a9034e48754e23"
	// event PostPonedReturn(address collateral, address indexed beneficiary, uint256 amount);
	TopicPostponedReturn = "0x8ab298b78a235f73eee230f82012c0cf4db76003eaabd16a0195f112e7d625c8"
	// event ForcedSale(address pos, uint256 amount, uint256 priceE36MinusDecimals);
	// Critical for Refresh: when collateral leaves a position via forced sale
	// post-expiration, no MintingUpdate fires, so without this topic the
	// stored CollateralBalance drifts away from on-chain truth forever.
	TopicForcedSale = "0x67a660133c1fb4c0bb0480a5e4a9919216684052f13f0713e88fa2fbbc81d082"

	// PositionV2.sol
	// event MintingUpdate(uint256 collateral, uint256 price, uint256 minted);
	TopicMintingUpdate = "0x9483a26ad376f30b5199a79e75df3bb05158c4ee32a348f53e83245a5e50c86e"
	// event PositionDenied(address indexed sender, string message);
	TopicPositionDenied = "0xaca80c800ec0d2aa9d9d31b7f886a1dd3067d4676abc637626a18ffb9381653d"

	// Standard OpenZeppelin Ownable event, emitted by Position contracts
	// during the CloneHelper handoff. Used by ResolveOwner.
	// event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
	TopicOwnership = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"
)

type Client struct {
	rpc *transport.Balancer
}

func New(rpc *transport.Balancer) *Client {
	return &Client{rpc: rpc}
}

// blockCacheMaxAge bounds how stale the balancer's last-seen block can be
// before we fall back to a real eth_blockNumber call. Mainnet blocks land
// every ~12s; if the balancer hasn't recorded anything in this window,
// either every endpoint is misbehaving or the indexer just started — both
// cases want a fresh RPC.
const blockCacheMaxAge = 20 * time.Second

// BlockNumber returns the current chain head. Prefers the balancer's
// in-memory last-seen block (updated by every cached call that returns a
// block number) so the indexer's per-tick head check doesn't cost an RPC.
// Falls back to a real eth_blockNumber if the cache is empty or stale.
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	if n, at := c.rpc.LastSeenBlock(); n > 0 && time.Since(at) < blockCacheMaxAge {
		return n, nil
	}
	var hexStr string
	if err := c.rpc.Call(ctx, "eth_blockNumber", []any{}, &hexStr); err != nil {
		return 0, err
	}
	return parseHexUint64(hexStr)
}

// ---- Logs ----

type LogFilter struct {
	FromBlock uint64
	ToBlock   uint64
	Addresses []string
	Topics    [][]string // outer = topic position, inner = OR alternatives
}

type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
}

func (l *Log) BlockNum() uint64 {
	n, _ := parseHexUint64(l.BlockNumber)
	return n
}

func (l *Log) LogIdx() uint64 {
	n, _ := parseHexUint64(l.LogIndex)
	return n
}

func (c *Client) GetLogs(ctx context.Context, f LogFilter) ([]Log, error) {
	params := map[string]any{
		"fromBlock": "0x" + strconv.FormatUint(f.FromBlock, 16),
		"toBlock":   "0x" + strconv.FormatUint(f.ToBlock, 16),
	}
	if len(f.Addresses) > 0 {
		params["address"] = f.Addresses
	}
	if len(f.Topics) > 0 {
		topics := make([]any, len(f.Topics))
		for i, t := range f.Topics {
			if len(t) == 1 {
				topics[i] = t[0]
			} else if len(t) > 1 {
				topics[i] = t
			}
		}
		params["topics"] = topics
	}
	var out []Log
	if err := c.rpc.Call(ctx, "eth_getLogs", []any{params}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ---- eth_call ----

func (c *Client) ethCall(ctx context.Context, to string, data []byte) ([]byte, error) {
	var hexResult string
	err := c.rpc.Call(ctx, "eth_call", []any{
		map[string]any{
			"to":   to,
			"data": "0x" + hex.EncodeToString(data),
		},
		"latest",
	}, &hexResult)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(strings.TrimPrefix(hexResult, "0x"))
}

// ---- Receipts (for CloneHelper trick) ----

type Receipt struct {
	Logs []Log `json:"logs"`
}

func (c *Client) GetTransactionReceipt(ctx context.Context, txHash string) (*Receipt, error) {
	var r Receipt
	if err := c.rpc.Call(ctx, "eth_getTransactionReceipt", []any{txHash}, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ---- Block (for batched tx sender lookup during bid indexing) ----

type blockTx struct {
	Hash string `json:"hash"`
	From string `json:"from"`
}

type blockResponse struct {
	Transactions []blockTx `json:"transactions"`
}

// TxSendersInBlock returns map[txHash]from for every tx in the given block.
// One eth_getBlockByNumber call replaces N eth_getTransactionByHash calls
// when indexing bids — we need tx.from since ChallengeAverted/Succeeded
// don't index the bidder.
func (c *Client) TxSendersInBlock(ctx context.Context, blockNum uint64) (map[string]string, error) {
	hexBlock := "0x" + strconv.FormatUint(blockNum, 16)
	var b blockResponse
	// true = include full tx objects (we need .from)
	if err := c.rpc.Call(ctx, "eth_getBlockByNumber", []any{hexBlock, true}, &b); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(b.Transactions))
	for _, tx := range b.Transactions {
		out[strings.ToLower(tx.Hash)] = strings.ToLower(tx.From)
	}
	return out, nil
}

// BlockHeader is what eth_getBlockByNumber returns when we ask for just headers.
type BlockHeader struct {
	Timestamp    string    `json:"timestamp"`
	Transactions []blockTx `json:"transactions"`
}

func (b *BlockHeader) Ts() int64 {
	n, _ := parseHexUint64(b.Timestamp)
	return int64(n)
}

// GetBlock fetches header + full txs in one shot. Use this when you need
// both timestamp and tx senders (challenge indexing does).
func (c *Client) GetBlock(ctx context.Context, blockNum uint64) (*BlockHeader, error) {
	hexBlock := "0x" + strconv.FormatUint(blockNum, 16)
	var b BlockHeader
	if err := c.rpc.Call(ctx, "eth_getBlockByNumber", []any{hexBlock, true}, &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// ---- helpers ----

func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 16, 64)
}

func TopicToAddress(topic string) string {
	t := strings.TrimPrefix(topic, "0x")
	if len(t) < 40 {
		return ""
	}
	return "0x" + strings.ToLower(t[len(t)-40:])
}

// callWithSelector — runs eth_call with just a 4-byte selector (no args).
func (c *Client) callWithSelector(ctx context.Context, to string, sel [4]byte) ([]byte, error) {
	return c.ethCall(ctx, to, sel[:])
}

var errMulticallFailed = errors.New("multicall call failed")
