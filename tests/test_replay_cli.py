import json
import random
import sqlite3
import subprocess
import sys
from pathlib import Path

import make_demo
import pytest
from encoder import (cpi_instruction, encode_event, gen_event, gen_pubkey, invoke_logs, logs_notification,
                     program_data_line, self_cpi_logs, synthetic_signature, tx_result)
from synthetic_idl import IDL, PROGRAM_ID

from pumpfun_stream.cli import main
from pumpfun_stream.idl import EVENT_IX_TAG, default_idl_path

ROOT = Path(__file__).resolve().parent.parent
needs_vendored_idl = pytest.mark.skipif(
    not (default_idl_path("pump").is_file() and default_idl_path("pump_amm").is_file()),
    reason="the demo is built from the vendored IDL files")


@pytest.fixture
def idl_file(tmp_path):
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(IDL))
    return path


def write_replay(path: Path) -> dict:
    rng = random.Random(21)
    a, b, c = (gen_event(IDL, "Small", rng) for _ in range(3))
    sig1, sig2, sig3 = (synthetic_signature(rng) for _ in range(3))
    note1 = logs_notification(sig1, 100, invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", a))]))
    logs2 = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", b))] + self_cpi_logs(PROGRAM_ID))
    note2 = logs_notification(sig2, 101, logs2)
    tx2 = {"jsonrpc": "2.0", "id": 9, "result": tx_result(
        sig2, [gen_pubkey(rng), PROGRAM_ID], rng, 101, logs2, [cpi_instruction(1, encode_event(IDL, "Small", c))])}
    failed = logs_notification(sig3, 102, invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", a))]),
                               err={"InstructionError": [0, "Custom"]})
    broken_tx = {"result": {"slot": 1, "transaction": ["!!!", "base64"], "meta": {"err": None}}}
    lines = [note1, note1, note2, tx2, failed, {"not": "an item"}, broken_tx]
    path.write_text("".join(json.dumps(x) + "\n" for x in lines) + "not json\n")
    return {"a": a, "b": b, "c": c, "sig1": sig1, "sig2": sig2}


def test_replay_end_to_end(tmp_path, idl_file, capsys):
    replay = tmp_path / "in.jsonl"
    want = write_replay(replay)
    out, db = tmp_path / "out.jsonl", tmp_path / "out.db"
    rc = main(["replay", str(replay), "--programs", "synthetic", "--idl", f"synthetic={idl_file}",
               "--out", str(out), "--sqlite", str(db), "--status-interval", "0"])
    assert rc == 0
    recs = [json.loads(line) for line in out.read_text().splitlines()]
    got = [(r["signature"], r["event_index"], r["fields"], r["source"]) for r in recs]
    assert got == [
        (want["sig1"], 0, want["a"], "replay"),
        (want["sig2"], 0, want["b"], "replay"),   # from the logs
        (want["sig2"], 1, want["c"], "replay"),   # emit_cpi, only in the full transaction
    ]
    err = capsys.readouterr().err
    assert "dup=2" in err and "failed_tx=1" in err and "unreadable_lines=2" in err
    assert "decode_errors=0" in err  # the broken transaction is counted as a bad item, not a crash
    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 3
    con.close()

    assert main(["stats", str(out)]) == 0
    text = capsys.readouterr().out
    assert "events: 3 in 2 transactions" in text and "synthetic Small" in text


@needs_vendored_idl
def test_demo_file_is_reproducible():
    assert make_demo.DEMO_PATH.read_text(encoding="utf-8") == make_demo.build()
    assert make_demo.DEMO_PATH.stat().st_size < 1_000_000


@needs_vendored_idl
def test_demo_replay(tmp_path, capsys):
    out = tmp_path / "demo.jsonl"
    assert main(["replay", str(make_demo.DEMO_PATH), "--out", str(out), "--status-interval", "0"]) == 0
    capsys.readouterr()
    main(["stats", str(out)])
    text = capsys.readouterr().out
    assert "events: 6 in 5 transactions" in text
    assert "unknown discriminators: 1" in text
    assert "decode errors: 0" in text
    assert "records with extra trailing bytes: 1" in text


def test_decode_command(idl_file, capsys):
    value = gen_event(IDL, "Small", random.Random(4))
    import base64

    from pumpfun_stream.base58 import b58encode

    raw = encode_event(IDL, "Small", value)
    assert main(["decode", base64.b64encode(raw).decode(), "--program", "synthetic",
                 "--idl", f"synthetic={idl_file}"]) == 0
    assert json.loads(capsys.readouterr().out)["fields"] == value
    assert main(["decode", b58encode(EVENT_IX_TAG + raw), "--base58", "--program", "synthetic",
                 "--idl", f"synthetic={idl_file}"]) == 0
    assert json.loads(capsys.readouterr().out)["event"] == "Small"
    assert main(["decode", base64.b64encode(b"\x00" * 12).decode(), "--program", "synthetic",
                 "--idl", f"synthetic={idl_file}"]) == 1


def test_missing_idl_gives_a_clear_error(tmp_path):
    with pytest.raises(SystemExit, match="pass --idl"):
        main(["replay", str(tmp_path / "x"), "--programs", "nothere"])


def test_python_dash_m_works():
    proc = subprocess.run([sys.executable, "-m", "pumpfun_stream", "--version"], cwd=ROOT,
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0 and "pumpfun-stream" in proc.stdout
