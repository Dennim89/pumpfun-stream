"""Event sources.  A source is anything with a `name`, a `live` flag and an async
`items()` generator that yields LogsItem or TxItem objects.

To plug in another feed (for example a Yellowstone gRPC subscription), write a class with
the same three members and pass it to Pipeline.run().
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Mapping, Protocol, Union


@dataclass
class LogsItem:
    """One transaction's log messages, as a logsNotification carries them."""
    signature: str
    slot: int | None
    logs: list[str]
    err: Any = None
    block_time: int | None = None
    source: str = "ws"


@dataclass
class TxItem:
    """One getTransaction result (encoding base64 or json)."""
    result: Mapping[str, Any]
    source: str = "rpc"


Item = Union[LogsItem, TxItem]


class Source(Protocol):
    name: str
    live: bool

    def items(self) -> AsyncIterator[Item]: ...


def item_from_json(obj: Any, source: str) -> Item | None:
    """Accept the shapes people save from RPC: a logsNotification message, its `result`,
    a bare {signature, logs} object, a getTransaction response or its `result`."""
    if not isinstance(obj, Mapping):
        return None
    if obj.get("method") == "logsNotification":
        obj = (obj.get("params") or {}).get("result") or {}
    elif "result" in obj and isinstance(obj["result"], Mapping) and "transaction" in obj["result"]:
        obj = obj["result"]
    if "transaction" in obj and "meta" in obj:
        return TxItem(obj, source)
    slot = obj.get("slot")
    if "value" in obj and isinstance(obj["value"], Mapping):
        slot = (obj.get("context") or {}).get("slot", slot)
        obj = obj["value"]
    if isinstance(obj.get("signature"), str) and isinstance(obj.get("logs"), list):
        return LogsItem(obj["signature"], obj.get("slot", slot), obj["logs"], obj.get("err"),
                        obj.get("blockTime", obj.get("block_time")), source)
    return None


class ReplaySource:
    """Read logs batches or raw transactions from a JSONL file, one object per line."""

    name = "replay"
    live = False

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.bad_lines = 0

    async def items(self) -> AsyncIterator[Item]:
        with self.path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = item_from_json(json.loads(line), self.name)
                except ValueError:
                    item = None
                if item is None:
                    self.bad_lines += 1
                    continue
                yield item
