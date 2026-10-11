"""Build examples/demo_replay.jsonl: synthetic pump.fun / PumpSwap traffic for the offline demo.

    python tests/make_demo.py            # rewrite the file
    python tests/make_demo.py --check    # exit 1 if the file is out of date

All keys and signatures are random bytes from a fixed seed.  Amounts are random small
numbers.  Nothing here is real network data.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from encoder import (cpi_instruction, encode_event, gen_event, gen_pubkey, invoke_logs,  # noqa: E402
                     logs_notification, program_data_line, self_cpi_logs, synthetic_signature, tx_result)

from pumpfun_stream import PROGRAM_IDS  # noqa: E402
from pumpfun_stream.idl import default_idl_path  # noqa: E402

DEMO_PATH = ROOT / "examples" / "demo_replay.jsonl"
SEED = 7
BLOCK_TIME = 1767225600  # 2026-01-01T00:00:00Z


def build() -> str:
    rng = random.Random(SEED)
    pump = json.loads(default_idl_path("pump").read_text(encoding="utf-8"))
    amm = json.loads(default_idl_path("pump_amm").read_text(encoding="utf-8"))
    pool = [gen_pubkey(rng) for _ in range(4)]
    pump_id, amm_id = PROGRAM_IDS["pump"], PROGRAM_IDS["pump_amm"]
    token = {"name": "Example Token", "symbol": "EXMPL", "uri": "https://example.com/token.json"}
    lines = []

    roles = {"mint": pool[0], "base_mint": pool[0], "user": pool[1], "creator": pool[2],
             "coin_creator": pool[2], "bonding_curve": pool[3], "pool": pool[3],
             "quote_mint": "So11111111111111111111111111111111111111112"}  # wrapped SOL, public

    def ev(doc, event_name, **over):
        value = gen_event(doc, event_name, rng, pool=pool, overrides=over)
        for key, val in roles.items():
            if key in value and key not in over:
                value[key] = val
        if "timestamp" in value:
            value["timestamp"] = str(BLOCK_TIME + len(lines))
        return encode_event(doc, event_name, value)

    # 1. a token is created (emit!, seen in the logs)
    sig = synthetic_signature(rng)
    lines.append(logs_notification(sig, 1000, invoke_logs(pump_id, [program_data_line(ev(pump, "CreateEvent", **token))])))
    # 2. a trade on the bonding curve; 3. the same transaction again from the second subscription
    sig = synthetic_signature(rng)
    trade = logs_notification(sig, 1001, invoke_logs(pump_id, [program_data_line(
        ev(pump, "TradeEvent", is_buy=True, ix_name="buy", shareholders=[]))]))
    lines += [trade, dict(trade, params=dict(trade["params"], subscription=2))]
    # 4. a PumpSwap buy
    sig = synthetic_signature(rng)
    lines.append(logs_notification(sig, 1002, invoke_logs(amm_id, [program_data_line(
        ev(amm, "BuyEvent", ix_name="buy"))]), sub=2))
    # 5. a full transaction: one emit! event in the logs, one emit_cpi! event in an inner instruction
    sig = synthetic_signature(rng)
    logs = invoke_logs(pump_id, [program_data_line(ev(pump, "TradeEvent", is_buy=False, ix_name="sell", shareholders=[]))]
                       + self_cpi_logs(pump_id))
    lines.append({"jsonrpc": "2.0", "id": 1, "result": tx_result(
        sig, [pool[1], pump_id], rng, 1003, logs, [cpi_instruction(1, ev(pump, "CompleteEvent"))],
        block_time=BLOCK_TIME)})
    # 6. an event this IDL does not know
    sig = synthetic_signature(rng)
    lines.append(logs_notification(sig, 1004, invoke_logs(pump_id, [program_data_line(rng.randbytes(24))])))
    # 7. a failed transaction: its events are skipped
    sig = synthetic_signature(rng)
    lines.append(logs_notification(sig, 1005, invoke_logs(pump_id, [program_data_line(
        ev(pump, "TradeEvent", is_buy=True, ix_name="buy", shareholders=[]))]),
        err={"InstructionError": [0, {"Custom": 6001}]}))
    # 8. a PumpSwap sell with 4 bytes the IDL does not describe (a newer program version)
    sig = synthetic_signature(rng)
    lines.append(logs_notification(sig, 1006, invoke_logs(amm_id, [program_data_line(
        ev(amm, "SellEvent", ix_name="sell") + b"\x01\x02\x03\x04")]), sub=2))
    return "".join(json.dumps(obj, separators=(",", ":")) + "\n" for obj in lines)


if __name__ == "__main__":
    text = build()
    if "--check" in sys.argv:
        sys.exit(0 if DEMO_PATH.read_text(encoding="utf-8") == text else 1)
    DEMO_PATH.write_text(text, encoding="utf-8")
    print(f"wrote {DEMO_PATH.relative_to(ROOT)} ({len(text)} bytes)")
