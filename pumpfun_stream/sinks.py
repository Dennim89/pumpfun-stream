"""Output writers and the in-memory dedup set."""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import OrderedDict
from typing import Mapping, TextIO

from .records import Record

SCHEMA_VERSION = "1"


class Dedup:
    """Remembers the last `maxsize` keys.  `seen(key)` returns True for a repeat."""

    def __init__(self, maxsize: int = 200_000):
        self.maxsize = maxsize
        self._keys: OrderedDict = OrderedDict()

    def seen(self, key) -> bool:
        if key in self._keys:
            self._keys.move_to_end(key)
            return True
        self._keys[key] = None
        if len(self._keys) > self.maxsize:
            self._keys.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._keys)


class JsonlSink:
    """One JSON object per line.  Path "-" writes to stdout."""

    def __init__(self, path: str, stream: TextIO | None = None):
        self.path = path
        if stream is not None:
            self.fh, self._own = stream, False
        elif path == "-":
            self.fh, self._own = sys.stdout, False
        else:
            self.fh, self._own = open(path, "a", encoding="utf-8"), True

    def write(self, record: Record) -> None:
        self.fh.write(record.to_json() + "\n")

    def flush(self) -> None:
        self.fh.flush()

    def close(self) -> None:
        self.flush()
        if self._own:
            self.fh.close()


class SqliteSink:
    """`events` table keyed by (signature, event_index) plus a `meta` key/value table."""

    def __init__(self, path: str, meta: Mapping[str, str] | None = None, batch: int = 500):
        self.conn = sqlite3.connect(path)
        self.batch = batch
        self._pending = 0
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                program      TEXT NOT NULL,
                event        TEXT NOT NULL,
                signature    TEXT NOT NULL,
                slot         INTEGER,
                block_time   INTEGER,
                event_index  INTEGER NOT NULL,
                received_at  TEXT NOT NULL,
                source       TEXT NOT NULL,
                fields_json  TEXT NOT NULL,
                extra_bytes  INTEGER NOT NULL,
                PRIMARY KEY (signature, event_index)
            );
            CREATE INDEX IF NOT EXISTS events_by_event ON events (program, event);
            CREATE INDEX IF NOT EXISTS events_by_slot ON events (slot);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        rows = {"schema_version": SCHEMA_VERSION, **(meta or {})}
        self.conn.executemany("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", rows.items())
        self.conn.commit()

    def write(self, r: Record) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (r.program, r.event, r.signature, r.slot, r.block_time, r.event_index, r.received_at,
             r.source, json.dumps(r.fields, separators=(",", ":"), ensure_ascii=False), r.extra_bytes),
        )
        self._pending += 1
        if self._pending >= self.batch:
            self.flush()

    def flush(self) -> None:
        self.conn.commit()
        self._pending = 0

    def close(self) -> None:
        self.flush()
        self.conn.close()
