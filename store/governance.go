package store

import (
	"database/sql"
	"encoding/json"
	"strings"
)

// ---------------- types ----------------
// All shapes mirror what api.frankencoin.com / ponder return so bootstrap
// stays a near-1:1 copy. Numbers come back as int64 / strings respectively.

// Minter mirrors /ecosystem/minter/list rows.
type Minter struct {
	ChainID           int    `json:"chainId"`
	TxHash            string `json:"txHash"`
	Minter            string `json:"minter"`
	ApplicationPeriod int64  `json:"applicationPeriod"`
	ApplicationFee    string `json:"applicationFee"` // bigint string
	ApplyMessage      string `json:"applyMessage"`
	ApplyDate         int64  `json:"applyDate"` // unix seconds
	Suggestor         string `json:"suggestor"`
	DenyMessage       string `json:"denyMessage,omitempty"`
	DenyDate          int64  `json:"denyDate,omitempty"`
	DenyTxHash        string `json:"denyTxHash,omitempty"`
	Vetor             string `json:"vetor,omitempty"`
}

// LeadrateApproved mirrors entries inside /savings/leadrate/rates list[chainId][module][].
// One row per (chainId, module, count) tuple — `count` is the proposal version.
type LeadrateApproved struct {
	ChainID      int    `json:"chainId"`
	Module       string `json:"module"`
	Count        int    `json:"count"`
	Created      int64  `json:"created"`
	Blockheight  int64  `json:"blockheight"`
	TxHash       string `json:"txHash"`
	ApprovedRate int    `json:"approvedRate"` // PPM (e.g. 35000 = 3.5%)
}

// LeadrateProposed mirrors /savings/leadrate/proposals list[chainId][module][].
// Same (chainId, module, count) key — join to LeadrateApproved by these three.
type LeadrateProposed struct {
	ChainID     int    `json:"chainId"`
	Module      string `json:"module"`
	Count       int    `json:"count"`
	Created     int64  `json:"created"`
	Blockheight int64  `json:"blockheight"`
	TxHash      string `json:"txHash"`
	Proposer    string `json:"proposer"`
	NextChange  int64  `json:"nextChange"` // unix sec when it'd take effect
	NextRate    int    `json:"nextRate"`   // PPM
}

// FPSHolder is the result of one ponder eRC20BalanceMappings row, filtered
// to the Equity contract.
type FPSHolder struct {
	Account string `json:"account"`
	Balance string `json:"balance"` // bigint string, 18dp
	Updated int64  `json:"updated"` // unix seconds (last balance change)
}

// EquityDelegation is one (owner, delegatedTo) edge from ponder.
type EquityDelegation struct {
	Owner       string `json:"owner"`
	DelegatedTo string `json:"delegatedTo"`
}

// ---------------- migration ----------------

func (s *Store) migrateGovernance() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS minters (
			tx_hash TEXT NOT NULL,
			minter  TEXT NOT NULL,
			data    TEXT NOT NULL,
			chain_id INTEGER NOT NULL,
			apply_date INTEGER NOT NULL,
			denied INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (tx_hash, minter)
		);
		CREATE INDEX IF NOT EXISTS idx_minters_chain ON minters(chain_id);
		CREATE INDEX IF NOT EXISTS idx_minters_date ON minters(apply_date DESC);

		CREATE TABLE IF NOT EXISTS leadrate_approved (
			chain_id INTEGER NOT NULL,
			module   TEXT NOT NULL,
			count    INTEGER NOT NULL,
			data     TEXT NOT NULL,
			created  INTEGER NOT NULL,
			PRIMARY KEY (chain_id, module, count)
		);
		CREATE INDEX IF NOT EXISTS idx_lr_approved_created ON leadrate_approved(created DESC);

		CREATE TABLE IF NOT EXISTS leadrate_proposed (
			chain_id INTEGER NOT NULL,
			module   TEXT NOT NULL,
			count    INTEGER NOT NULL,
			data     TEXT NOT NULL,
			created  INTEGER NOT NULL,
			PRIMARY KEY (chain_id, module, count)
		);
		CREATE INDEX IF NOT EXISTS idx_lr_proposed_created ON leadrate_proposed(created DESC);

		CREATE TABLE IF NOT EXISTS fps_holders (
			account TEXT PRIMARY KEY,
			balance TEXT NOT NULL,
			updated INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_fps_holders_balance ON fps_holders(CAST(balance AS REAL) DESC);

		CREATE TABLE IF NOT EXISTS equity_delegations (
			owner        TEXT PRIMARY KEY,
			delegated_to TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_delegations_to ON equity_delegations(delegated_to);
	`)
	return err
}

// ---------------- minters ----------------

func (s *Store) BulkUpsertMinters(items []*Minter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Snapshot replace — minters table is small (~50 rows ever), and the API
	// returns the full history every call. Simpler than diffing.
	if _, err := tx.Exec(`DELETE FROM minters`); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO minters(tx_hash, minter, data, chain_id, apply_date, denied)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range items {
		m.Minter = strings.ToLower(m.Minter)
		m.Suggestor = strings.ToLower(m.Suggestor)
		if m.Vetor != "" {
			m.Vetor = strings.ToLower(m.Vetor)
		}
		blob, err := json.Marshal(m)
		if err != nil {
			return err
		}
		denied := 0
		if m.DenyDate > 0 {
			denied = 1
		}
		if _, err := stmt.Exec(m.TxHash, m.Minter, string(blob), m.ChainID, m.ApplyDate, denied); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AllMinters() ([]*Minter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`SELECT data FROM minters ORDER BY apply_date DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Minter{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		m := &Minter{}
		if err := json.Unmarshal([]byte(blob), m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) MinterCount() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM minters`).Scan(&n)
	return n, err
}

// ---------------- leadrate ----------------

func (s *Store) BulkUpsertLeadrateApproved(items []*LeadrateApproved) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM leadrate_approved`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO leadrate_approved(chain_id, module, count, data, created)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range items {
		r.Module = strings.ToLower(r.Module)
		blob, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(r.ChainID, r.Module, r.Count, string(blob), r.Created); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) BulkUpsertLeadrateProposed(items []*LeadrateProposed) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM leadrate_proposed`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO leadrate_proposed(chain_id, module, count, data, created)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range items {
		r.Module = strings.ToLower(r.Module)
		r.Proposer = strings.ToLower(r.Proposer)
		blob, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(r.ChainID, r.Module, r.Count, string(blob), r.Created); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AllLeadrateApproved() ([]*LeadrateApproved, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return queryLeadrateApproved(s.db, `SELECT data FROM leadrate_approved ORDER BY created DESC`)
}

func (s *Store) AllLeadrateProposed() ([]*LeadrateProposed, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return queryLeadrateProposed(s.db, `SELECT data FROM leadrate_proposed ORDER BY created DESC`)
}

func queryLeadrateApproved(db *sql.DB, q string, args ...any) ([]*LeadrateApproved, error) {
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*LeadrateApproved{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		r := &LeadrateApproved{}
		if err := json.Unmarshal([]byte(blob), r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func queryLeadrateProposed(db *sql.DB, q string, args ...any) ([]*LeadrateProposed, error) {
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*LeadrateProposed{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		r := &LeadrateProposed{}
		if err := json.Unmarshal([]byte(blob), r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------- fps holders + delegations ----------------

func (s *Store) BulkUpsertFPSHolders(items []*FPSHolder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM fps_holders`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO fps_holders(account, balance, updated) VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, h := range items {
		h.Account = strings.ToLower(h.Account)
		if _, err := stmt.Exec(h.Account, h.Balance, h.Updated); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// FPSHoldersTop returns the top N holders by balance, descending. The balance
// column is a bigint stored as text — we CAST to REAL for ordering. That's
// lossy past ~2^53 but fine for ranking (FPS supply is ~9k tokens; even
// rebased it never approaches that limit).
func (s *Store) FPSHoldersTop(limit int) ([]*FPSHolder, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT account, balance, updated FROM fps_holders
		ORDER BY CAST(balance AS REAL) DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*FPSHolder{}
	for rows.Next() {
		h := &FPSHolder{}
		if err := rows.Scan(&h.Account, &h.Balance, &h.Updated); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) BulkUpsertDelegations(items []*EquityDelegation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM equity_delegations`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO equity_delegations(owner, delegated_to) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, d := range items {
		d.Owner = strings.ToLower(d.Owner)
		d.DelegatedTo = strings.ToLower(d.DelegatedTo)
		if _, err := stmt.Exec(d.Owner, d.DelegatedTo); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AllDelegations() ([]*EquityDelegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`SELECT owner, delegated_to FROM equity_delegations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*EquityDelegation{}
	for rows.Next() {
		d := &EquityDelegation{}
		if err := rows.Scan(&d.Owner, &d.DelegatedTo); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) FPSHolderCount() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM fps_holders`).Scan(&n)
	return n, err
}

func (s *Store) DelegationCount() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM equity_delegations`).Scan(&n)
	return n, err
}
