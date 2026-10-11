"""Load Anchor IDL files (0.30+ format and the older legacy format)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .borsh import DecodeError, Reader, decode_fields, decode_typedef

# Anchor's EVENT_IX_TAG: the u64 0x1d9acb512ea545e4 (= first 8 bytes of
# sha256("anchor:event"), read big-endian) written little-endian.  emit_cpi! puts it in
# front of the event bytes in a self-invoked instruction.
EVENT_IX_TAG = bytes.fromhex("e445a52e51cb9a1d")

PACKAGE_IDL_DIR = Path(__file__).resolve().parent / "idl"


def event_discriminator(name: str) -> bytes:
    """First 8 bytes of sha256("event:" + EventName)."""
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


@dataclass
class EventDef:
    name: str
    discriminator: bytes
    fields: Any = None  # legacy inline fields; None means "look up the type by name"


@dataclass
class Idl:
    name: str
    address: str | None
    events: dict[bytes, EventDef]
    types: dict[str, Any]
    sha256: str
    version: str | None = None
    warnings: list[str] = field(default_factory=list)

    def decode_event(self, payload: bytes):
        """Return (EventDef, fields, extra_bytes).

        Raises KeyError for an unknown discriminator and DecodeError for a payload that
        does not match the layout.
        """
        if len(payload) < 8:
            raise DecodeError(f"payload of {len(payload)} bytes is shorter than a discriminator")
        ev = self.events[payload[:8]]
        r = Reader(payload, 8)
        if ev.fields is not None:
            values = decode_fields(r, ev.fields, self.types)
        else:
            typedef = self.types.get(ev.name)
            if typedef is None:
                raise DecodeError(f"IDL has no type for event {ev.name}")
            values = decode_typedef(r, typedef, self.types)
        return ev, values, r.remaining


def load_idl(source: str | Path | Mapping[str, Any], name: str | None = None) -> Idl:
    if isinstance(source, Mapping):
        raw = json.dumps(source, sort_keys=True).encode()
        doc = source
    else:
        raw = Path(source).read_bytes()
        doc = json.loads(raw)
    meta = doc.get("metadata") or {}
    idl_name = name or meta.get("name") or doc.get("name") or "unknown"
    address = doc.get("address") or meta.get("address")
    types = {t["name"]: t for t in doc.get("types") or []}
    events: dict[bytes, EventDef] = {}
    warnings = []
    for ev in doc.get("events") or []:
        disc = event_discriminator(ev["name"])
        listed = ev.get("discriminator")
        if listed is not None and bytes(listed) != disc:
            warnings.append(f"{idl_name}: listed discriminator of {ev['name']} differs from the computed one")
        events[disc] = EventDef(ev["name"], disc, ev.get("fields"))
    return Idl(
        name=idl_name,
        address=address,
        events=events,
        types=types,
        sha256=hashlib.sha256(raw).hexdigest(),
        version=meta.get("version") or doc.get("version"),
        warnings=warnings,
    )


def default_idl_path(program: str) -> Path:
    return PACKAGE_IDL_DIR / f"{program}.json"
