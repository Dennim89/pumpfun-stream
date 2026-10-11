# Vendored IDL files

| File | Source | Fetched | sha256 of the original file |
|---|---|---|---|
| `pump.json` | https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump.json | 2026-10-09 | `6a19bad7758fb822cee79a91575e51a7bd60391c670200408676643e00a79a6d` |
| `pump_amm.json` | https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump_amm.json | 2026-10-09 | `b7d8c57a4d9c4dd0109a9ab893052352333252d4eedced10ac091d4d66cab89b` |

These are the Anchor IDLs that pump.fun publishes for its bonding-curve program and its
PumpSwap AMM program. They were trimmed with `scripts/trim_idl.py` to the parts the decoder
reads (`address`, `metadata`, `events`, `types`); instructions, accounts and errors were
removed. No field or type was edited.

License: at the time of fetching, the `pump-fun/pump-public-docs` repository did not
declare a license. The files describe a public on-chain interface. They are not covered
by this project's MIT license. If you redistribute this project and are unsure, delete this
folder and pass your own copies with `--idl pump=... --idl pump_amm=...`.

To refresh: download the two files again, run `scripts/trim_idl.py` on each, update this
table and run the tests.
