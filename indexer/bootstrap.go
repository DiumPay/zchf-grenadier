package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/store"
)

type apiPosition struct {
	Version              int         `json:"version"`
	Position             string      `json:"position"`
	Owner                string      `json:"owner"`
	Zchf                 string      `json:"zchf"`
	Collateral           string      `json:"collateral"`
	Price                string      `json:"price"`
	Created              int64       `json:"created"`
	IsOriginal           bool        `json:"isOriginal"`
	IsClone              bool        `json:"isClone"`
	Denied               bool        `json:"denied"`
	DenyDate             int64       `json:"denyDate"`
	Closed               bool        `json:"closed"`
	Original             string      `json:"original"`
	Parent               string      `json:"parent"`
	MinimumCollateral    string      `json:"minimumCollateral"`
	AnnualInterestPPM    int         `json:"annualInterestPPM"`
	RiskPremiumPPM       int         `json:"riskPremiumPPM"`
	ReserveContribution  int         `json:"reserveContribution"`
	Start                int64       `json:"start"`
	Cooldown             json.Number `json:"cooldown"`
	Expiration           int64       `json:"expiration"`
	ChallengePeriod      int64       `json:"challengePeriod"`
	CollateralName       string      `json:"collateralName"`
	CollateralSymbol     string      `json:"collateralSymbol"`
	CollateralDecimals   int         `json:"collateralDecimals"`
	CollateralBalance    string      `json:"collateralBalance"`
	LimitForPosition     string      `json:"limitForPosition"`
	LimitForClones       string      `json:"limitForClones"`
	AvailableForClones   string      `json:"availableForClones"`
	AvailableForMinting  string      `json:"availableForMinting"`
	AvailableForPosition string      `json:"availableForPosition"`
	Minted               string      `json:"minted"`
}

type apiResponse struct {
	Num  int           `json:"num"`
	List []apiPosition `json:"list"`
}

// Bootstrap seeds the store from the peer list the first time we run.
// Skips if the store already has positions. Tries each configured peer
// in order (your own grenadiers first, official as fallback).
func Bootstrap(ctx context.Context, st *store.Store, ch *chain.Client) error {
	count, err := st.Count()
	if err != nil {
		return err
	}
	if count > 0 {
		// already seeded — resume from last_block
		return nil
	}

	fmt.Println("[bootstrap] fetching positions from peer list...")
	t0 := time.Now()

	var api apiResponse
	source, err := FetchAndDecodeFromPeers(ctx, EndpointPositions, 30*time.Second, &api)
	if err != nil {
		return fmt.Errorf("bootstrap positions: %w", err)
	}

	positions := make([]*store.Position, 0, len(api.List))
	for _, p := range api.List {
		if p.Version != 2 {
			continue // V2 only
		}
		positions = append(positions, convertAPI(&p))
	}

	blockNum, err := ch.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap block: %w", err)
	}

	if err := st.BulkUpsert(positions, int64(blockNum)); err != nil {
		return err
	}
	if err := st.SetLastBlock(blockNum); err != nil {
		return err
	}

	fmt.Printf("[bootstrap] seeded %d positions from %s in %v, resuming from block %d\n",
		len(positions), source.Name, time.Since(t0), blockNum)
	return nil
}

func convertAPI(p *apiPosition) *store.Position {
	// cooldown can be scientific notation for "infinity" (closed positions)
	var cooldown int64
	if s := p.Cooldown.String(); s != "" {
		var n int64
		if _, err := fmt.Sscanf(s, "%d", &n); err == nil {
			cooldown = n
		} else {
			cooldown = 1 << 62 // sentinel for "forever"
		}
	}

	return &store.Position{
		Version:              p.Version,
		Position:             strings.ToLower(p.Position),
		Owner:                strings.ToLower(p.Owner),
		Zchf:                 strings.ToLower(p.Zchf),
		Collateral:           strings.ToLower(p.Collateral),
		Price:                p.Price,
		Created:              p.Created,
		IsOriginal:           p.IsOriginal,
		IsClone:              p.IsClone,
		Denied:               p.Denied,
		DenyDate:             p.DenyDate,
		Closed:               p.Closed,
		Original:             strings.ToLower(p.Original),
		Parent:               strings.ToLower(p.Parent),
		MinimumCollateral:    p.MinimumCollateral,
		AnnualInterestPPM:    p.AnnualInterestPPM,
		RiskPremiumPPM:       p.RiskPremiumPPM,
		ReserveContribution:  p.ReserveContribution,
		Start:                p.Start,
		Cooldown:             cooldown,
		Expiration:           p.Expiration,
		ChallengePeriod:      p.ChallengePeriod,
		CollateralName:       p.CollateralName,
		CollateralSymbol:     p.CollateralSymbol,
		CollateralDecimals:   p.CollateralDecimals,
		CollateralBalance:    p.CollateralBalance,
		LimitForPosition:     p.LimitForPosition,
		LimitForClones:       p.LimitForClones,
		AvailableForClones:   p.AvailableForClones,
		AvailableForMinting:  p.AvailableForMinting,
		AvailableForPosition: p.AvailableForPosition,
		Minted:               p.Minted,
	}
}
