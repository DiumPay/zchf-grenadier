package store

import (
	"database/sql"
	"encoding/json"
	"strings"
)

// Challenge mirrors the api.frankencoin.com /challenges/list row shape so
// the bootstrap maps 1:1 and downstream consumers see the same fields.
type Challenge struct {
	ID                 string `json:"id"` // e.g. "0xpos-challenge-N"
	Position           string `json:"position"`
	Number             uint64 `json:"number"`
	TxHash             string `json:"txHash"`
	Challenger         string `json:"challenger"`
	Start              int64  `json:"start"`              // unix seconds, challenge start
	Created            int64  `json:"created"`            // unix seconds, also start in api
	Duration           int64  `json:"duration"`           // challenge period seconds
	Size               string `json:"size"`               // original challenge size (collateral, bigint string)
	LiqPrice           string `json:"liqPrice"`           // position liq price at challenge time (36 - decimals digits)
	Bids               int    `json:"bids"`               // total bid count
	FilledSize         string `json:"filledSize"`         // collateral consumed so far
	AcquiredCollateral string `json:"acquiredCollateral"` // collateral actually bought (phase-2 only)
	Status             string `json:"status"`             // 'Active' | 'Averted' | 'Success'
	Version            int    `json:"version"`            // 1 or 2
	Block              int64  `json:"block,omitempty"`
}

// Bid mirrors /challenges/bids/list row shape.
type Bid struct {
	ID                 string `json:"id"` // e.g. "0xpos-challenge-N-bid-K"
	Position           string `json:"position"`
	Number             uint64 `json:"number"`    // challenge number
	NumberBid          uint64 `json:"numberBid"` // bid index within challenge
	TxHash             string `json:"txHash"`
	Bidder             string `json:"bidder"`
	Created            int64  `json:"created"` // unix seconds
	BidType            string `json:"bidType"` // 'Averted' | 'Succeeded'
	Bid                string `json:"bid"`     // ZCHF amount
	Price              string `json:"price"`   // unit price at bid time
	FilledSize         string `json:"filledSize"`
	AcquiredCollateral string `json:"acquiredCollateral"`
	ChallengeSize      string `json:"challengeSize"` // total challenge size for context
	Version            int    `json:"version"`
	Block              int64  `json:"block,omitempty"`
}

// migrateChallenges is called from Open after migrate(). New tables only —
// no ALTER, no migration path needed since user resets DB.
func (s *Store) migrateChallenges() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS challenges (
			id TEXT PRIMARY KEY,
			data TEXT NOT NULL,
			challenger TEXT NOT NULL,
			position TEXT NOT NULL,
			status TEXT NOT NULL,
			updated_block INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_chal_challenger ON challenges(challenger);
		CREATE INDEX IF NOT EXISTS idx_chal_position ON challenges(position);
		CREATE INDEX IF NOT EXISTS idx_chal_status ON challenges(status);

		CREATE TABLE IF NOT EXISTS bids (
			id TEXT PRIMARY KEY,
			data TEXT NOT NULL,
			bidder TEXT NOT NULL,
			position TEXT NOT NULL,
			block INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_bids_bidder ON bids(bidder);
		CREATE INDEX IF NOT EXISTS idx_bids_position ON bids(position);
	`)
	return err
}

// ---------------- challenges ----------------

func (s *Store) UpsertChallenge(c *Challenge, block int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	c.Challenger = strings.ToLower(c.Challenger)
	c.Position = strings.ToLower(c.Position)

	blob, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT INTO challenges(id, data, challenger, position, status, updated_block)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			data=excluded.data,
			challenger=excluded.challenger,
			position=excluded.position,
			status=excluded.status,
			updated_block=excluded.updated_block
	`, c.ID, string(blob), c.Challenger, c.Position, c.Status, block)
	return err
}

// BulkUpsertChallenges — single transaction, used by the bootstrap.
func (s *Store) BulkUpsertChallenges(list []*Challenge, block int64) error {
	if len(list) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO challenges(id, data, challenger, position, status, updated_block)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			data=excluded.data,
			challenger=excluded.challenger,
			position=excluded.position,
			status=excluded.status,
			updated_block=excluded.updated_block
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range list {
		c.Challenger = strings.ToLower(c.Challenger)
		c.Position = strings.ToLower(c.Position)
		blob, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(c.ID, string(blob), c.Challenger, c.Position, c.Status, block); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetChallenge returns nil, nil if not found.
func (s *Store) GetChallenge(id string) (*Challenge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var blob string
	err := s.db.QueryRow(`SELECT data FROM challenges WHERE id = ?`, id).Scan(&blob)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c := &Challenge{}
	if err := json.Unmarshal([]byte(blob), c); err != nil {
		return nil, err
	}
	return c, nil
}

func (s *Store) ChallengesByChallenger(addr string) ([]*Challenge, error) {
	return s.queryChallenges(`SELECT data FROM challenges WHERE challenger = ?`, strings.ToLower(addr))
}

func (s *Store) ChallengesByPosition(addr string) ([]*Challenge, error) {
	return s.queryChallenges(`SELECT data FROM challenges WHERE position = ?`, strings.ToLower(addr))
}

func (s *Store) ActiveChallenges() ([]*Challenge, error) {
	return s.queryChallenges(`SELECT data FROM challenges WHERE status = ?`, "Active")
}

func (s *Store) AllChallenges() ([]*Challenge, error) {
	return s.queryChallenges(`SELECT data FROM challenges`)
}

func (s *Store) ChallengeCount() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM challenges`).Scan(&n)
	return n, err
}

func (s *Store) queryChallenges(query string, args ...any) ([]*Challenge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Challenge{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		c := &Challenge{}
		if err := json.Unmarshal([]byte(blob), c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---------------- bids ----------------

func (s *Store) UpsertBid(b *Bid) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b.Bidder = strings.ToLower(b.Bidder)
	b.Position = strings.ToLower(b.Position)

	blob, err := json.Marshal(b)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT INTO bids(id, data, bidder, position, block)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			data=excluded.data,
			bidder=excluded.bidder,
			position=excluded.position,
			block=excluded.block
	`, b.ID, string(blob), b.Bidder, b.Position, b.Block)
	return err
}

// BulkUpsertBids — single transaction, used by the bootstrap.
func (s *Store) BulkUpsertBids(list []*Bid) error {
	if len(list) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO bids(id, data, bidder, position, block)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			data=excluded.data,
			bidder=excluded.bidder,
			position=excluded.position,
			block=excluded.block
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, b := range list {
		b.Bidder = strings.ToLower(b.Bidder)
		b.Position = strings.ToLower(b.Position)
		blob, err := json.Marshal(b)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(b.ID, string(blob), b.Bidder, b.Position, b.Block); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) BidsByBidder(addr string) ([]*Bid, error) {
	return s.queryBids(`SELECT data FROM bids WHERE bidder = ? ORDER BY block DESC`, strings.ToLower(addr))
}

func (s *Store) BidsByPosition(addr string) ([]*Bid, error) {
	return s.queryBids(`SELECT data FROM bids WHERE position = ? ORDER BY block DESC`, strings.ToLower(addr))
}

func (s *Store) BidCount() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM bids`).Scan(&n)
	return n, err
}

func (s *Store) queryBids(query string, args ...any) ([]*Bid, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Bid{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		b := &Bid{}
		if err := json.Unmarshal([]byte(blob), b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
