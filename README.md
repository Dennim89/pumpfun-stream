# pumpfun-stream

Streams Solana transactions that touch the pump.fun bonding-curve program and the PumpSwap AMM
program, decodes their Anchor events with the published IDL files, and writes one normalised
record per event to JSONL or SQLite.

It is for people who build analytics, dashboards, research datasets or their own tools on
pump.fun data and want the raw events in a plain format. It is a data tool only: it does not
trade, rank tokens, compute prices or send transactions. It works with any Solana RPC that
supports `logsSubscribe`.

**Status: experimental.** Checked against mainnet on 2026-10-09: one minute on the public Solana
RPC with `--rps 2` decoded 15,805 events of 23 types, with zero decode errors and zero unknown
events. The decoder, the replay path and the reconnect logic also pass offline tests with synthetic
data. See [Limitations](#limitations).

Sample output from the offline demo (synthetic data in `examples/demo_replay.jsonl`, Linux x86_64,
Python 3.13):

```text
$ pumpfun-stream replay examples/demo_replay.jsonl --out events.jsonl
[done] events=6 reconnects=0 decode_errors=0 unknown=1 dup=1 failed_tx=1 queue=0/10000 fetch_queue=0/1000 dropped=0

$ pumpfun-stream stats events.jsonl
events: 6 in 5 transactions
slots: 1000..1006
  pump TradeEvent     2
  pump CompleteEvent  1
  pump CreateEvent    1
  pump_amm BuyEvent   1
  pump_amm SellEvent  1
records with extra trailing bytes: 1
unknown discriminators: 1
  pump 454eeef22f8a155d  1
decode errors: 0
```

The demo file holds eight items: a duplicate notification (dropped by dedup), a failed
transaction (skipped), an event the IDL does not know (counted, kept as `_unknown`), a PumpSwap
event with four extra trailing bytes (decoded, `extra_bytes: 4`), and one full transaction whose
`CompleteEvent` exists only as an `emit_cpi!` inner instruction.

Built with AI coding agents (Claude, Codex).

## Quickstart

```bash
git clone <this repository> pumpfun-stream && cd pumpfun-stream
python -m venv .venv && . .venv/bin/activate
pip install -e .
pumpfun-stream replay examples/demo_replay.jsonl --out events.jsonl
pumpfun-stream stats events.jsonl
```

`python -m pumpfun_stream ...` works the same as `pumpfun-stream ...`.

Live, with your own RPC endpoint:

```bash
pumpfun-stream run --ws wss://your-rpc.example/ --http https://your-rpc.example/ \
    --out events.jsonl --sqlite events.db
```

## Commands

| Command | What it does |
|---|---|
| `run --ws URL [--http URL] [--full-tx]` | Subscribes with `logsSubscribe` (one subscription per program), decodes events from the logs, and fetches transactions with `getTransaction` when they may hold `emit_cpi!` events (or every transaction with `--full-tx`). Prints a status line to stderr every 10 s. Stops cleanly on Ctrl-C or SIGTERM. |
| `replay FILE` | Decodes a JSONL file of saved items. Each line may be a `logsNotification` message, its `result`, a bare `{"signature", "logs", "slot"}` object, or a `getTransaction` response (`encoding` `base64` or `json`). |
| `decode PAYLOAD [--program pump] [--base58]` | Decodes one payload: the base64 text after `Program data:`, or base58 inner-instruction data (the `emit_cpi!` tag is stripped). Exit code 1 if it is not a known event. |
| `stats FILE` | Counts per event type, unknown discriminators and decode errors in a JSONL output file. |

Common options: `--programs pump,pump_amm`, `--idl NAME=PATH` (repeatable), `--out FILE` (`-` for
stdout, `''` to turn JSONL off; the file is appended to), `--sqlite FILE`, `--no-diagnostics`
(do not write `_unknown` and `_decode_error` records; they are still counted),
`--status-interval SECONDS`.

Settings can also come from environment variables; see `.env.example`. Command-line flags win.

## Output formats

**JSONL**, one record per event (the `CompleteEvent` from the demo):

```json
{"program":"pump","event":"CompleteEvent","signature":"1111111111111111111111116sXCZDtTPXHoKTgrJNfN7MS7MUNBYtiG9aNsbUWWQFuoAtL4pY9nyw4","slot":1003,"block_time":1767225600,"event_index":1,"received_at":"2026-10-09T08:26:22.181Z","source":"replay","fields":{"user":"3w43shingStocjN6p6pZUi3a7vD46W4nfgEpCJwKj9yw","mint":"4pMpYS3iEyR3tn8BeqvqxB7QCULegaiUC6puppPaaE8q","bonding_curve":"3aUYBZQmfs4MsAP2oWoNuTj9ThCuhgCVcThpZQFv3HdS","timestamp":"1767225604","quote_mint":"So11111111111111111111111111111111111111112"},"extra_bytes":0}
```

| Field | Meaning |
|---|---|
| `program` | `pump` or `pump_amm` (the IDL name) |
| `event` | Event name from the IDL, or `_unknown` / `_decode_error` |
| `signature`, `slot`, `block_time` | Transaction signature, slot, block time (`null` when the source does not carry it; logs notifications do not) |
| `event_index` | Position of the event in the transaction: `emit!` events from the logs first, then `emit_cpi!` events from inner instructions, both in transaction order. Counts events of the loaded programs only. |
| `received_at` | Local UTC time when the record was made |
| `source` | `ws`, `rpc` (fetched with `getTransaction`) or `replay` |
| `fields` | Decoded fields. u64/i64/u128/i128 are decimal strings, pubkeys are base58, `bytes` is hex. |
| `extra_bytes` | Bytes left after the fields the IDL describes (a newer program version may append fields) |

The dedup key is `(signature, event_index)`. Dedup is in memory (the last 200 000 keys by
default) and in SQLite (primary key), not across separate runs of a JSONL file.

**SQLite**: table `events` with the same columns, `fields_json` instead of `fields`, primary key
`(signature, event_index)`. Table `meta` holds the schema version, package version, and the
sha256, version and program id of each IDL used. One row from the demo:

```text
('pump_amm', 'SellEvent', '1111111111111111111111112E4ivoitHUuEV1nbTqvaPdUZc1jjZoGfX6vtLb9sScMvKgQnsgqPDZ8', 1006, 0, 'replay', 4, '{"timestamp":"1767225607","base_amount_in":"353975088","min_quote_amount_out":"6')
```

(columns `program, event, signature, slot, event_index, source, extra_bytes`, and the first 80
characters of `fields_json`).

**Status line** on stderr, from `run` against a local fake WebSocket server:

```text
[status] events=3 eps=3.0 reconnects=0 decode_errors=0 unknown=1 dup=1 failed_tx=0 queue=0/10000 fetch_queue=0/1000 dropped=0
```

`eps` is events per second since the previous line, `dropped` counts items lost to a full queue.

## How decoding works

The decoder reads Anchor IDL JSON (the 0.30+ format and the older one) instead of hard-coding
layouts, so a new IDL file is enough when pump.fun changes an event.

- The event discriminator is the first 8 bytes of `sha256("event:" + EventName)`. It is computed;
  the tests check it against the discriminators listed in the vendored IDLs.
- `emit!` events are log lines `Program data: <base64>` whose bytes are discriminator + Borsh
  fields. A line belongs to the program at the top of the invoke stack built from
  `Program <id> invoke [n]` / `success` / `failed` lines.
- `emit_cpi!` events are inner instructions from the program to itself whose data is the 8-byte
  Anchor event tag `e4 45 a5 2e 51 cb 9a 1d`, then the discriminator, then the fields. They do not
  appear in the logs, so `run` fetches the transaction when the logs show the program invoking
  itself (or always, with `--full-tx`).
- Borsh types: u8 to u128 and i8 to i128 (u256/i256 too), f32, f64, bool, pubkey (also
  `publicKey`), string, bytes, vec, option, coption, fixed arrays, and `defined` structs, enums
  and type aliases. Generic types are not supported.
- An unknown discriminator never stops the stream: it is counted per program and written as an
  `_unknown` record. A payload shorter than its IDL layout is written as `_decode_error`.
- Events from failed transactions are skipped and counted (`failed_tx`).

## Network behaviour

- Reconnects use exponential backoff with full jitter: a random delay between 0 and
  `min(60 s, 0.5 s × 2^attempt)`. The attempt counter resets only after a connection stayed up for
  a full minute, so a server that accepts and drops at once is not hammered.
- HTTP 429 is honoured on the WebSocket handshake and on `getTransaction`: the client waits at
  least `Retry-After` (capped at 600 s). For `getTransaction` the rate limiter is paused for the
  same time, so the other workers wait too.
- `getTransaction` calls go through a token-bucket limit (`--rps`, default 5 per second), a
  bounded fetch queue (`--fetch-queue-size`) and a fixed number of workers (`--fetch-workers`).
  A full queue drops and counts instead of blocking the WebSocket reader.
- `getTransaction` asks for JSON encoding with `maxSupportedTransactionVersion` 1, so the node
  lists the account keys and new transaction versions need no wire parser here (mainnet carried
  version-1 transactions when this was checked in October 2026). `--max-tx-version` changes it.
- A connection with no message for `--idle-timeout` seconds (default 120) is closed and reopened.

The program does nothing else on the network. It never signs or sends transactions.

## IDL files

`pumpfun_stream/idl/` holds copies of the IDLs that pump.fun publishes in
`pump-fun/pump-public-docs` (fetched 2026-10-09, trimmed to events and types; see
`pumpfun_stream/idl/SOURCE.md`). That repository did not declare a license when the files were
fetched. To use other versions, pass `--idl pump=path/to/pump.json --idl pump_amm=path/to/pump_amm.json`.
Full, untrimmed IDL files work too.

## Adding a source

A source is any object with `name`, `live` and an async `items()` generator that yields
`LogsItem` or `TxItem` (see `pumpfun_stream/sources.py`). A Yellowstone gRPC source would map
each subscribed transaction update to a `TxItem` and pass the source to `Pipeline.run()`. It is
not included: no Python client package for it was found on PyPI when this was written.

## Limitations

- Checked against mainnet for one minute only (see Status). Field layouts come from the published
  IDLs; if the deployed programs change, you will see `_decode_error` or `_unknown` records rather
  than wrong values, but check a sample against a block explorer before relying on it.
- pump.fun carries hundreds of transactions per second. On a free public RPC the `getTransaction`
  queue cannot keep up, so full-transaction fetches are dropped and counted (`dropped`); events in
  the logs are still decoded. For complete `emit_cpi!` coverage, use an RPC plan that allows a
  higher `--rps`.
- Logs-only mode misses `emit_cpi!` events unless `--http` is given. If an RPC truncates long
  logs (`Log truncated`), the transaction is fetched when `--http` is given; otherwise events
  after the cut are lost.
- An IDL newer than the on-chain data (fewer bytes than the layout) gives `_decode_error`; use the
  IDL version that matches the slots you read.
- `block_time` is `null` for events that come from WebSocket logs.
- Dedup is per run. Appending to an existing JSONL file can repeat records; SQLite ignores them.
- `logsSubscribe` gives no backfill: events during a disconnect are not recovered.
- No Yellowstone gRPC source (see above).

## Tests

```bash
pip install -e . pytest
python -m pytest -q
```

The tests run offline: socket connections are blocked, except to 127.0.0.1 in the tests that
start a fake WebSocket server. Fixtures are synthetic (random bytes from fixed seeds, encoded by
a Borsh encoder in `tests/encoder.py`). `python tests/make_demo.py` rebuilds the demo file; a
test checks that the committed file matches.

## License

MIT, see `LICENSE`. The vendored IDL files are pump.fun's and are not covered by it (see above).
Not affiliated with pump.fun.
