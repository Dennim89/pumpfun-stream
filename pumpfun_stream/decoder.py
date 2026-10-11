"""Turn logs and transactions into decoded event records."""
from __future__ import annotations

import base64
import binascii
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .base58 import b58decode
from .borsh import DecodeError
from .idl import EVENT_IX_TAG, Idl
from .txparse import TxFormatError, signatures_and_keys

UNKNOWN = "_unknown"
DECODE_ERROR = "_decode_error"

_INVOKE = re.compile(r"^Program (\S+) invoke \[(\d+)\]")
_EXIT = re.compile(r"^Program (\S+) (?:success|failed)")
_DATA = "Program data: "


@dataclass
class Payload:
    """Raw event bytes found in a transaction, before IDL decoding."""
    program: str          # IDL name, e.g. "pump"
    data: bytes | None    # None when the log line itself was not valid base64
    via: str              # "log" (emit!) or "cpi" (emit_cpi!)
    error: str | None = None


@dataclass
class Decoded:
    program: str
    event: str
    fields: Any
    extra_bytes: int


@dataclass
class LogScan:
    payloads: list[Payload]
    self_cpi: set[str]      # programs that invoked themselves: they may carry emit_cpi events
    truncated: bool


@dataclass
class TxScan:
    signature: str | None
    slot: int | None
    block_time: int | None
    err: Any
    payloads: list[Payload]
    truncated: bool


@dataclass
class Counters:
    unknown: Counter = field(default_factory=Counter)        # per program
    decode_errors: Counter = field(default_factory=Counter)  # per program


class Decoder:
    def __init__(self, idls: Mapping[str, Idl]):
        self.idls = dict(idls)
        self.by_address = {idl.address: name for name, idl in self.idls.items() if idl.address}
        self.counters = Counters()

    # -- payloads -----------------------------------------------------------------
    def decode(self, p: Payload) -> Decoded:
        """Decode one payload.  Never raises: problems become _unknown / _decode_error records."""
        if p.data is None:
            self.counters.decode_errors[p.program] += 1
            return Decoded(p.program, DECODE_ERROR, {"error": p.error, "via": p.via}, 0)
        idl = self.idls[p.program]
        disc = p.data[:8].hex()
        try:
            ev, values, extra = idl.decode_event(p.data)
        except KeyError:
            self.counters.unknown[p.program] += 1
            return Decoded(p.program, UNKNOWN, {"discriminator": disc, "length": len(p.data), "via": p.via}, 0)
        except DecodeError as exc:
            self.counters.decode_errors[p.program] += 1
            name = None
            if len(p.data) >= 8 and p.data[:8] in idl.events:
                name = idl.events[p.data[:8]].name
            return Decoded(p.program, DECODE_ERROR,
                           {"event": name, "discriminator": disc, "error": str(exc), "via": p.via}, 0)
        return Decoded(p.program, ev.name, values, extra)

    def decode_raw(self, program: str, data: bytes) -> Decoded:
        """Decode bytes given on the command line; strips the emit_cpi tag when present."""
        via = "log"
        if data[:8] == EVENT_IX_TAG:
            data, via = data[8:], "cpi"
        return self.decode(Payload(program, data, via))

    # -- logs ---------------------------------------------------------------------
    def scan_logs(self, logs: Iterable[str]) -> LogScan:
        stack: list[str] = []
        payloads: list[Payload] = []
        self_cpi: set[str] = set()
        truncated = False
        for line in logs:
            if not isinstance(line, str):
                continue
            if line.startswith(_DATA):
                prog = self.by_address.get(stack[-1]) if stack else None
                if prog is None:
                    continue
                try:
                    data = b"".join(base64.b64decode(chunk, validate=True) for chunk in line[len(_DATA):].split())
                    payloads.append(Payload(prog, data, "log"))
                except (binascii.Error, ValueError) as exc:
                    payloads.append(Payload(prog, None, "log", f"bad base64 in log: {exc}"))
                continue
            m = _INVOKE.match(line)
            if m:
                pid = m.group(1)
                if stack and stack[-1] == pid and pid in self.by_address:
                    self_cpi.add(self.by_address[pid])
                stack.append(pid)
                continue
            m = _EXIT.match(line)
            if m:
                pid = m.group(1)
                if pid in stack:
                    while stack and stack.pop() != pid:
                        pass
                continue
            if line.startswith("Log truncated"):
                truncated = True
        return LogScan(payloads, self_cpi, truncated)

    # -- full transactions ----------------------------------------------------------
    def scan_transaction(self, result: Mapping[str, Any]) -> TxScan:
        """A getTransaction result: emit! events from the logs first, then emit_cpi! events
        from inner instructions, each group in transaction order."""
        meta = result.get("meta") or {}
        sigs, keys = signatures_and_keys(result)
        logs = self.scan_logs(meta.get("logMessages") or [])
        payloads = list(logs.payloads)
        groups = sorted(meta.get("innerInstructions") or [], key=lambda g: g.get("index", 0))
        for group in groups:
            for ix in group.get("instructions") or []:
                idx = ix.get("programIdIndex")
                if not isinstance(idx, int) or idx >= len(keys):
                    continue
                prog = self.by_address.get(keys[idx])
                if prog is None or not isinstance(ix.get("data"), str):
                    continue
                try:
                    data = b58decode(ix["data"])
                except ValueError as exc:
                    payloads.append(Payload(prog, None, "cpi", f"bad base58 in instruction: {exc}"))
                    continue
                if data[:8] == EVENT_IX_TAG:
                    payloads.append(Payload(prog, data[8:], "cpi"))
        return TxScan(
            signature=sigs[0] if sigs else None,
            slot=result.get("slot"),
            block_time=result.get("blockTime"),
            err=meta.get("err"),
            payloads=payloads,
            truncated=logs.truncated,
        )


__all__ = ["Decoder", "Payload", "Decoded", "TxScan", "LogScan", "UNKNOWN", "DECODE_ERROR", "TxFormatError"]
