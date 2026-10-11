"""The normalised event record and its JSON form."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

COLUMNS = ("program", "event", "signature", "slot", "block_time", "event_index",
           "received_at", "source", "fields", "extra_bytes")


@dataclass
class Record:
    program: str
    event: str
    signature: str
    slot: int | None
    block_time: int | None
    event_index: int
    received_at: str
    source: str
    fields: Any
    extra_bytes: int

    @property
    def key(self) -> tuple[str, int]:
        return (self.signature, self.event_index)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)


def utc_iso(ts: float) -> str:
    """Unix seconds -> 2026-01-01T00:00:00.000Z"""
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
