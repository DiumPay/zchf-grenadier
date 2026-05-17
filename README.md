# Frankencoin mini Indexer (Grenadier)

A lightweight indexer for Frankencoin. Tracks positions, challenges, bids and governance.

Currently hosted at https://grenadier.frankencoin.win

It works as a mirror of the official Frankencoin API and Ponder. If one of them goes down, this one still alive. More sources means better reliability...

## Run it

```
docker compose up -d
```

No env file, no API key, no Postgres. Just a SQLite file in `./data`. Install and play. Works behind Caddy, nginx or Cloudflare out of the box.

## Endpoints

Positions
- `GET /positions`
- `GET /positions/curated`
- `GET /positions/monitored`
- `GET /positions/owner/:addr`

Challenges and bids
- `GET /challenges`
- `GET /challenges/active`
- `GET /challenges/challenger/:addr`
- `GET /challenges/position/:addr`
- `GET /bids/bidder/:addr`
- `GET /bids/position/:addr`

Governance
- `GET /governance` (everything in one call)
- `GET /governance/minters`
- `GET /governance/leadrate`
- `GET /governance/fps-holders`
- `GET /governance/delegations`

Prices (tracked tickers only)
- `GET /prices/list`
- `GET /prices/ticker/:sym`

Health
- `GET /health`

## Features

- Pool of 24 public RPC endpoints from https://chainlist.org, raced and load balanced, with circuit breakers and block-aware caching.
- `eth_getLogs` is address-scoped and chunked in parallel, so it scales past the ~1024 address limit on most RPCs.
- Bootstrap pulls from peer mirrors first, then falls back to the official sources.
- SQLite in WAL mode for concurrent reads.
- Two-tier rate limiting per IP.