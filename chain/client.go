package chain

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/DiumPay/zchf-grenadier/transport"
)

const Multicall3Address = "0xcA11bde05977b3631167028862bE2a173976CA11"

const (
	TopicPositionOpened   = "0x0fcf2cc70f4e23dd5b40d8a420186fd886f8dfde64eb4b8a2dafe2c52727b69c"
	TopicMintingUpdate    = "0x9483a26ad376f30b5199a79e75df3bb05158c4ee32a348f53e83245a5e50c86e"
	TopicPositionDenied   = "0x9416d6a3a9c0c25b16fb33e2a4d27ff42a4880466cd9c2ec5e4c45c6e9b65c8a"
	TopicOwnership        = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"
	TopicChallengeStarted = "0xc4b384b2c5ca32c8e77081f4083be594a1ea9ba34f208a9f9a458f70608585f5"
	TopicChallengeAverted = "0x1eee30d91b773ac47d7485a3acb6bcd8c7c9cd8d95301b1af361baf5f0991d2e"
	TopicChallengeSucceed = "0x7d3a26e8d43c5b70f86266bfa26c212e3c097716ff7240ccb6a9034e48754e23"
)

type Client struct {
	rpc *transport.Balancer
}

func New(rpc *transport.Balancer) *Client {
	return &Client{rpc: rpc}
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
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
