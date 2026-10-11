"""Test helpers: a Borsh encoder driven by IDL types, a seeded value generator, and builders
for synthetic logs and transactions.  Everything here is written for the tests; none of it
comes from real network data."""
from __future__ import annotations

import base64
import random
import struct
from typing import Any, Mapping

from pumpfun_stream.base58 import b58decode, b58encode
from pumpfun_stream.borsh import INT_SIZES, defined_name, type_name
from pumpfun_stream.idl import EVENT_IX_TAG, event_discriminator


# -- encoder -------------------------------------------------------------------------------
def encode_value(ty: Any, value: Any, types: Mapping[str, Any]) -> bytes:
    if isinstance(ty, str):
        name = type_name(ty)
        if name in INT_SIZES:
            size, signed = INT_SIZES[name]
            return int(value).to_bytes(size, "little", signed=signed)
        if name == "bool":
            return b"\x01" if value else b"\x00"
        if name == "pubkey":
            raw = b58decode(value)
            assert len(raw) == 32
            return raw
        if name == "string":
            raw = value.encode()
            return len(raw).to_bytes(4, "little") + raw
        if name == "bytes":
            raw = bytes.fromhex(value)
            return len(raw).to_bytes(4, "little") + raw
        if name == "f32":
            return struct.pack("<f", value)
        if name == "f64":
            return struct.pack("<d", value)
        raise ValueError(ty)
    (kind, arg), = ty.items()
    if kind == "vec":
        return len(value).to_bytes(4, "little") + b"".join(encode_value(arg, v, types) for v in value)
    if kind == "option":
        return b"\x00" if value is None else b"\x01" + encode_value(arg, value, types)
    if kind == "coption":
        return b"\x00" * 4 if value is None else (1).to_bytes(4, "little") + encode_value(arg, value, types)
    if kind == "array":
        inner, n = arg
        assert len(value) == n
        return b"".join(encode_value(inner, v, types) for v in value)
    if kind == "defined":
        return encode_typedef(types[defined_name(arg)], value, types)
    raise ValueError(ty)


def encode_fields(fields, value, types) -> bytes:
    if not fields:
        return b""
    if isinstance(fields[0], Mapping):
        return b"".join(encode_value(f["type"], value[f["name"]], types) for f in fields)
    return b"".join(encode_value(f, v, types) for f, v in zip(fields, value))


def encode_typedef(typedef, value, types) -> bytes:
    body = typedef.get("type", typedef)
    if body["kind"] == "struct":
        return encode_fields(body.get("fields"), value, types)
    if body["kind"] == "enum":
        names = [v["name"] for v in body["variants"]]
        if isinstance(value, str):
            return bytes([names.index(value)])
        (vname, inner), = value.items()
        variant = body["variants"][names.index(vname)]
        return bytes([names.index(vname)]) + encode_fields(variant["fields"], inner, types)
    if body["kind"] == "type":
        return encode_value(body["alias"], value, types)
    raise ValueError(body)


def idl_types(idl_doc: Mapping) -> dict:
    return {t["name"]: t for t in idl_doc.get("types") or []}


def event_fields(idl_doc: Mapping, name: str):
    for ev in idl_doc.get("events") or []:
        if ev["name"] == name and "fields" in ev:
            return ev["fields"]
    return idl_types(idl_doc)[name]["type"]["fields"]


def encode_event(idl_doc: Mapping, name: str, value: Mapping) -> bytes:
    """discriminator + Borsh fields, as emit! writes them."""
    return event_discriminator(name) + encode_fields(event_fields(idl_doc, name), value, idl_types(idl_doc))


# -- seeded value generator (values in the decoder's JSON form) -------------------------------
def gen_pubkey(rng: random.Random) -> str:
    return b58encode(rng.randbytes(32))


def gen_value(ty: Any, rng: random.Random, types: Mapping[str, Any], depth: int = 0,
              pool: list[str] | None = None) -> Any:
    """Random values over the full range of each type.  With `pool` (demo mode): small
    integers, empty vecs, None options and pubkeys picked from the pool."""
    if isinstance(ty, str):
        name = type_name(ty)
        if name in INT_SIZES:
            size, signed = INT_SIZES[name]
            bits = size * 8
            lo, hi = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if signed else (0, (1 << bits) - 1)
            if pool is not None:
                v = rng.randint(0, min(hi, 10**9))
            else:
                v = rng.choice([lo, hi, 0, rng.randint(lo, hi), rng.randint(lo, hi)])
            return str(v) if size > 4 else v
        if name == "bool":
            return rng.random() < 0.5
        if name == "pubkey":
            return rng.choice(pool) if pool is not None else gen_pubkey(rng)
        if name == "string":
            if pool is not None:
                return "example"
            return "".join(rng.choice("abcdefgh XYZ-é") for _ in range(rng.randint(0, 12)))
        if name == "bytes":
            return rng.randbytes(rng.randint(0, 6)).hex()
        if name in ("f32", "f64"):
            return rng.randint(-1000, 1000) / 4  # exactly representable in both widths
        raise ValueError(ty)
    (kind, arg), = ty.items()
    if kind == "vec":
        n = 0 if pool is not None or depth >= 3 else rng.randint(0, 3)
        return [gen_value(arg, rng, types, depth + 1, pool) for _ in range(n)]
    if kind in ("option", "coption"):
        if pool is not None or rng.random() < 0.3:
            return None
        return gen_value(arg, rng, types, depth + 1, pool)
    if kind == "array":
        return [gen_value(arg[0], rng, types, depth + 1, pool) for _ in range(arg[1])]
    if kind == "defined":
        return gen_typedef(types[defined_name(arg)], rng, types, depth + 1, pool)
    raise ValueError(ty)


def gen_fields(fields, rng, types, depth=0, pool=None):
    if not fields:
        return {}
    if isinstance(fields[0], Mapping):
        return {f["name"]: gen_value(f["type"], rng, types, depth, pool) for f in fields}
    return [gen_value(f, rng, types, depth, pool) for f in fields]


def gen_typedef(typedef, rng, types, depth=0, pool=None):
    body = typedef.get("type", typedef)
    if body["kind"] == "struct":
        return gen_fields(body.get("fields"), rng, types, depth, pool)
    if body["kind"] == "enum":
        variant = rng.choice(body["variants"])
        if not variant.get("fields"):
            return variant["name"]
        return {variant["name"]: gen_fields(variant["fields"], rng, types, depth, pool)}
    if body["kind"] == "type":
        return gen_value(body["alias"], rng, types, depth, pool)
    raise ValueError(body)


def gen_event(idl_doc: Mapping, event_name: str, rng: random.Random, pool: list[str] | None = None,
              overrides: Mapping | None = None) -> dict:
    value = gen_fields(event_fields(idl_doc, event_name), rng, idl_types(idl_doc), 0, pool)
    value.update(overrides or {})
    return value


# -- synthetic logs and transactions ------------------------------------------------------------
def synthetic_signature(rng: random.Random) -> str:
    """64 bytes: 24 zero bytes, then 40 random ones.  The leading run of '1's marks it as fake."""
    return b58encode(bytes(24) + rng.randbytes(40))


def program_data_line(payload: bytes) -> str:
    return "Program data: " + base64.b64encode(payload).decode()


def invoke_logs(program_id: str, inner: list[str], depth: int = 1) -> list[str]:
    return ([f"Program {program_id} invoke [{depth}]", "Program log: synthetic instruction"] + inner
            + [f"Program {program_id} consumed 1000 of 200000 compute units", f"Program {program_id} success"])


def self_cpi_logs(program_id: str, depth: int = 2) -> list[str]:
    return [f"Program {program_id} invoke [{depth}]",
            f"Program {program_id} consumed 100 of 190000 compute units",
            f"Program {program_id} success"]


def compact_u16(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def wire_transaction(signature: str, keys: list[str], rng: random.Random, version0: bool = False,
                     program_index: int = 1) -> bytes:
    """A minimal transaction: one signature, the given static keys, one instruction."""
    sig = b58decode(signature)
    assert len(sig) == 64
    msg = bytearray()
    if version0:
        msg.append(0x80)
    msg += bytes([1, 0, 1])
    msg += compact_u16(len(keys)) + b"".join(b58decode(k) for k in keys)
    msg += rng.randbytes(32)  # recent blockhash
    msg += compact_u16(1) + bytes([program_index]) + compact_u16(1) + bytes([0]) + compact_u16(8) + rng.randbytes(8)
    if version0:
        msg += compact_u16(0)  # no address table lookups
    return compact_u16(1) + sig + bytes(msg)


def cpi_instruction(program_index: int, payload: bytes) -> dict:
    return {"programIdIndex": program_index, "accounts": [], "data": b58encode(EVENT_IX_TAG + payload),
            "stackHeight": 2}


def tx_result(signature: str, keys: list[str], rng: random.Random, slot: int, logs: list[str],
              inner: list[dict], err=None, block_time=None, version0=False, loaded=None,
              program_index: int = 1) -> dict:
    raw = wire_transaction(signature, keys, rng, version0, program_index)
    return {
        "slot": slot,
        "blockTime": block_time,
        "version": 0 if version0 else "legacy",
        "transaction": [base64.b64encode(raw).decode(), "base64"],
        "meta": {
            "err": err,
            "logMessages": logs,
            "innerInstructions": [{"index": 0, "instructions": inner}] if inner else [],
            "loadedAddresses": loaded or {"writable": [], "readonly": []},
        },
    }


def logs_notification(signature: str, slot: int, logs: list[str], err=None, sub: int = 1) -> dict:
    return {"jsonrpc": "2.0", "method": "logsNotification", "params": {
        "result": {"context": {"slot": slot}, "value": {"signature": signature, "err": err, "logs": logs}},
        "subscription": sub}}
