package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
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

// Store wraps the SQLite handle.
//
// Concurrency: SQLite (WAL) handles many concurrent readers + one writer at
// the file level; database/sql manages the connection pool. Reads take no
// Go-level lock. writeMu serializes write paths so bursts can't trip
// SQLITE_BUSY and to make the contract explicit.
type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

func Open(path string) (*Store, error) {
	// Pragmas:
	//   journal_mode(WAL)  — concurrent readers + one writer
	//   busy_timeout(5000) — wait up to 5s on contention before SQLITE_BUSY
	//   cache_size(-64000) — 64 MiB page cache (negative units = KiB)
	//   temp_store(MEMORY) — keep temp tables off disk
	//   synchronous: left at default (FULL). Do not relax: durability gate.
	db, err := sql.Open("sqlite3", "file:"+path+
		"?_pragma=journal_mode(WAL)"+
		"&_pragma=busy_timeout(5000)"+
		"&_pragma=cache_size(-64000)"+
		"&_pragma=temp_store(MEMORY)")
	if err != nil {
		return nil, err
	}
	// SQLite serializes writes at the file level; a small pool is correct.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	if err := s.migrateChallenges(); err != nil {
		return nil, err
	}
	if err := s.migrateGovernance(); err != nil {
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

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

// GetPosition returns nil, nil if not found.
// Used by the challenge indexer to read ChallengePeriod / Price without an
// 18-call multicall — the row was already written this tick by Refresh.
func (s *Store) GetPosition(addr string) (*Position, error) {
	var blob string
	err := s.db.QueryRow(`SELECT data FROM positions WHERE position = ?`, strings.ToLower(addr)).Scan(&blob)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p := &Position{}
	if err := json.Unmarshal([]byte(blob), p); err != nil {
		return nil, err
	}
	return p, nil
}

// AllRaw returns stored blobs without unmarshal/remarshal. /positions just
// re-emits them; parsing to structs and back is wasted work.
func (s *Store) AllRaw() ([]json.RawMessage, error) {
	rows, err := s.db.Query(`SELECT data FROM positions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, err
		}
		cp := make(json.RawMessage, len(blob)) // copy: driver may reuse buffer
		copy(cp, blob)
		out = append(out, cp)
	}
	return out, rows.Err()
}

// AllLive: positions that are not closed and not denied. The predicate is
// pushed to SQL so dead rows never get parsed for the live-set endpoints.
func (s *Store) AllLive() ([]*Position, error) {
	rows, err := s.db.Query(`SELECT data FROM positions WHERE closed = 0 AND denied = 0`)
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
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM positions`).Scan(&n)
	return n, err
}

// --- meta (last scanned block, etc) ---

func (s *Store) GetMeta(k string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM meta WHERE k = ?`, k).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(k, v string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
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
	n, perr := strconv.ParseUint(v, 10, 64)
	if perr != nil {
		return 0, perr
	}
	return n, nil
}

func (s *Store) SetLastBlock(n uint64) error {
	return s.SetMeta("last_block", fmt.Sprintf("%d", n))
}
