package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// Position mirrors the frankencoin api shape so swap is trivial.
// bigints are strings, addresses are lowercase hex.
type Position struct {
	Version              int    `json:"version"`
	Position             string `json:"position"`
	Owner                string `json:"owner"`
	Zchf                 string `json:"zchf"`
	Collateral           string `json:"collateral"`
	Price                string `json:"price"`
	Created              int64  `json:"created"`
	IsOriginal           bool   `json:"isOriginal"`
	IsClone              bool   `json:"isClone"`
	Denied               bool   `json:"denied"`
	DenyDate             int64  `json:"denyDate"`
	Closed               bool   `json:"closed"`
	Original             string `json:"original"`
	Parent               string `json:"parent,omitempty"`
	MinimumCollateral    string `json:"minimumCollateral"`
	AnnualInterestPPM    int    `json:"annualInterestPPM"`
	RiskPremiumPPM       int    `json:"riskPremiumPPM"`
	ReserveContribution  int    `json:"reserveContribution"`
	Start                int64  `json:"start"`
	Cooldown             int64  `json:"cooldown"`
	Expiration           int64  `json:"expiration"`
	ChallengePeriod      int64  `json:"challengePeriod"`
	CollateralName       string `json:"collateralName"`
	CollateralSymbol     string `json:"collateralSymbol"`
	CollateralDecimals   int    `json:"collateralDecimals"`
	CollateralBalance    string `json:"collateralBalance"`
	LimitForPosition     string `json:"limitForPosition"`
	LimitForClones       string `json:"limitForClones"`
	AvailableForClones   string `json:"availableForClones"`
	AvailableForMinting  string `json:"availableForMinting"`
	AvailableForPosition string `json:"availableForPosition"`
	Minted               string `json:"minted"`
}

type Store struct {
	db *sql.DB
	mu sync.RWMutex
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS positions (
			position TEXT PRIMARY KEY,
			data TEXT NOT NULL,
			owner TEXT NOT NULL,
			closed INTEGER NOT NULL,
			denied INTEGER NOT NULL,
			updated_block INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_owner ON positions(owner);
		CREATE INDEX IF NOT EXISTS idx_open ON positions(closed, denied);

		CREATE TABLE IF NOT EXISTS meta (
			k TEXT PRIMARY KEY,
			v TEXT NOT NULL
		);
	`)
	return err
}

func (s *Store) Upsert(p *Position, block int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p.Position = strings.ToLower(p.Position)
	p.Owner = strings.ToLower(p.Owner)

	blob, err := json.Marshal(p)
	if err != nil {
		return err
	}

	closed := 0
	if p.Closed {
		closed = 1
	}
	denied := 0
	if p.Denied {
		denied = 1
	}

	_, err = s.db.Exec(`
		INSERT INTO positions(position, data, owner, closed, denied, updated_block)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(position) DO UPDATE SET
			data=excluded.data,
			owner=excluded.owner,
			closed=excluded.closed,
			denied=excluded.denied,
			updated_block=excluded.updated_block
	`, p.Position, string(blob), p.Owner, closed, denied, block)
	return err
}

func (s *Store) BulkUpsert(positions []*Position, block int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO positions(position, data, owner, closed, denied, updated_block)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(position) DO UPDATE SET
			data=excluded.data,
			owner=excluded.owner,
			closed=excluded.closed,
			denied=excluded.denied,
			updated_block=excluded.updated_block
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range positions {
		p.Position = strings.ToLower(p.Position)
		p.Owner = strings.ToLower(p.Owner)
		blob, err := json.Marshal(p)
		if err != nil {
			return err
		}
		closed := 0
		if p.Closed {
			closed = 1
		}
		denied := 0
		if p.Denied {
			denied = 1
		}
		if _, err := stmt.Exec(p.Position, string(blob), p.Owner, closed, denied, block); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) All() ([]*Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`SELECT data FROM positions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []*Position{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		p := &Position{}
		if err := json.Unmarshal([]byte(blob), p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) ByOwner(owner string) ([]*Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`SELECT data FROM positions WHERE owner = ?`, strings.ToLower(owner))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []*Position{}
	for rows.Next() {
		var blob string
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		p := &Position{}
		if err := json.Unmarshal([]byte(blob), p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) AllAddresses() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`SELECT position FROM positions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) Count() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM positions`).Scan(&n)
	return n, err
}

// --- meta (last scanned block, etc) ---

func (s *Store) GetMeta(k string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var v string
	err := s.db.QueryRow(`SELECT v FROM meta WHERE k = ?`, k).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(k, v string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		INSERT INTO meta(k, v) VALUES (?, ?)
		ON CONFLICT(k) DO UPDATE SET v=excluded.v
	`, k, v)
	return err
}

func (s *Store) GetLastBlock() (uint64, error) {
	v, err := s.GetMeta("last_block")
	if err != nil || v == "" {
		return 0, err
	}
	var n uint64
	_, err = fmt.Sscanf(v, "%d", &n)
	return n, err
}

func (s *Store) SetLastBlock(n uint64) error {
	return s.SetMeta("last_block", fmt.Sprintf("%d", n))
}
