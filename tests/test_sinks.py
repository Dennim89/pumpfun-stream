import io
import json
import sqlite3

from pumpfun_stream.records import Record, utc_iso
from pumpfun_stream.sinks import Dedup, JsonlSink, SqliteSink


def rec(sig="sig-a", index=0, event="Small", fields=None):
    return Record("synthetic", event, sig, 10, None, index, utc_iso(0), "replay",
                  fields if fields is not None else {"value": "18446744073709551615"}, 0)


def test_utc_iso():
    assert utc_iso(1767225600.5) == "2026-01-01T00:00:00.500Z"


def test_dedup_is_bounded():
    d = Dedup(maxsize=2)
    assert not d.seen(("a", 0))
    assert d.seen(("a", 0))
    assert not d.seen(("a", 1))
    assert not d.seen(("b", 0))  # evicts ("a", 0)
    assert len(d) == 2
    assert not d.seen(("a", 0))


def test_jsonl_writer_one_object_per_line(tmp_path):
    buf = io.StringIO()
    sink = JsonlSink("-", stream=buf)
    sink.write(rec())
    sink.write(rec(index=1, fields={"name": "é"}))
    sink.close()
    lines = buf.getvalue().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert list(first) == ["program", "event", "signature", "slot", "block_time", "event_index",
                           "received_at", "source", "fields", "extra_bytes"]
    assert first["fields"]["value"] == "18446744073709551615"
    assert "é" in lines[1]

    path = tmp_path / "out.jsonl"
    file_sink = JsonlSink(str(path))
    file_sink.write(rec())
    file_sink.close()
    assert json.loads(path.read_text())["signature"] == "sig-a"


def test_sqlite_writer(tmp_path):
    path = tmp_path / "events.db"
    sink = SqliteSink(str(path), {"idl_sha256:synthetic": "abc"}, batch=1)
    sink.write(rec())
    sink.write(rec())  # same (signature, event_index): ignored
    sink.write(rec(index=1))
    sink.close()
    con = sqlite3.connect(path)
    rows = con.execute("SELECT signature, event_index, fields_json FROM events ORDER BY event_index").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("sig-a", 0), ("sig-a", 1)]
    assert json.loads(rows[0][2]) == {"value": "18446744073709551615"}
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["idl_sha256:synthetic"] == "abc" and meta["schema_version"] == "1"
    con.close()
