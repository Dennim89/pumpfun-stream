"""Just enough of the Solana transaction wire format to list signatures and account keys."""
from __future__ import annotations

import base64
from typing import Any, Mapping

from .base58 import b58encode


class TxFormatError(ValueError):
    pass


def _compact_u16(data: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    for i in range(3):
        if pos >= len(data):
            raise TxFormatError("truncated compact-u16")
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, pos
        shift += 7
    raise TxFormatError("compact-u16 too long")


def _take(data: bytes, pos: int, n: int) -> tuple[bytes, int]:
    if pos + n > len(data):
        raise TxFormatError("truncated transaction")
    return data[pos:pos + n], pos + n


def parse_wire(raw: bytes) -> tuple[list[str], list[str]]:
    """Return (signatures, static account keys), both base58, for a legacy or v0 transaction."""
    nsig, pos = _compact_u16(raw, 0)
    sigs = []
    for _ in range(nsig):
        sig, pos = _take(raw, pos, 64)
        sigs.append(b58encode(sig))
    if pos >= len(raw):
        raise TxFormatError("missing message")
    if raw[pos] & 0x80:
        version = raw[pos] & 0x7F
        if version != 0:
            raise TxFormatError(f"transaction version {version} is not parsed here; fetch it with encoding json")
        pos += 1  # v0 prefix; v0 has the same header and key list layout as legacy
    _, pos = _take(raw, pos, 3)  # header
    nkeys, pos = _compact_u16(raw, pos)
    keys = []
    for _ in range(nkeys):
        key, pos = _take(raw, pos, 32)
        keys.append(b58encode(key))
    return sigs, keys


def signatures_and_keys(result: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """Signatures and the full account key list (static + loaded) of a getTransaction result."""
    tx = result.get("transaction")
    if isinstance(tx, list) and len(tx) == 2 and tx[1] == "base64":
        sigs, keys = parse_wire(base64.b64decode(tx[0]))
    elif isinstance(tx, Mapping):  # encoding "json"
        sigs = list(tx.get("signatures") or [])
        keys = [k["pubkey"] if isinstance(k, Mapping) else k for k in tx["message"]["accountKeys"]]
    else:
        raise TxFormatError("unsupported transaction encoding (use base64 or json)")
    loaded = (result.get("meta") or {}).get("loadedAddresses") or {}
    keys = keys + list(loaded.get("writable") or []) + list(loaded.get("readonly") or [])
    return sigs, keys
