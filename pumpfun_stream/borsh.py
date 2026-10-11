"""A Borsh decoder driven by Anchor IDL type descriptions.

Supported: u8..u128, i8..i128 (u256/i256 too), f32, f64, bool, pubkey (also the legacy
spelling publicKey), string, bytes, vec, option, coption, fixed arrays, and `defined`
structs, enums and type aliases.

JSON mapping: integers wider than 32 bits become decimal strings (no float rounding),
pubkeys become base58, `bytes` becomes hex.
"""
from __future__ import annotations

import struct
from typing import Any, Mapping

from .base58 import b58encode

INT_SIZES = {
    "u8": (1, False), "u16": (2, False), "u32": (4, False), "u64": (8, False),
    "u128": (16, False), "u256": (32, False),
    "i8": (1, True), "i16": (2, True), "i32": (4, True), "i64": (8, True),
    "i128": (16, True), "i256": (32, True),
}
# Integers wider than this many bytes are written to JSON as strings.
_SMALL_INT_BYTES = 4
MAX_DEPTH = 64


class DecodeError(ValueError):
    """The payload does not match the IDL layout."""


class Reader:
    def __init__(self, data: bytes, pos: int = 0):
        self.data = data
        self.pos = pos

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    def take(self, n: int) -> bytes:
        if n < 0 or self.pos + n > len(self.data):
            raise DecodeError(f"unexpected end of data: need {n} bytes at offset {self.pos}, have {self.remaining}")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "little")


def type_name(ty: Any) -> str:
    """Normalise a primitive type name (legacy IDLs say publicKey)."""
    if ty == "publicKey":
        return "pubkey"
    return ty


def defined_name(spec: Any) -> str:
    """`{"defined": "X"}` (legacy) or `{"defined": {"name": "X"}}` (Anchor 0.30+)."""
    if isinstance(spec, str):
        return spec
    if isinstance(spec, Mapping) and "name" in spec:
        if spec.get("generics"):
            raise DecodeError("generic defined types are not supported")
        return spec["name"]
    raise DecodeError(f"bad defined type: {spec!r}")


def decode_value(r: Reader, ty: Any, types: Mapping[str, Any], depth: int = 0) -> Any:
    if depth > MAX_DEPTH:
        raise DecodeError("type nesting too deep")
    if isinstance(ty, str):
        name = type_name(ty)
        if name in INT_SIZES:
            size, signed = INT_SIZES[name]
            value = int.from_bytes(r.take(size), "little", signed=signed)
            return str(value) if size > _SMALL_INT_BYTES else value
        if name == "bool":
            b = r.take(1)[0]
            if b > 1:
                raise DecodeError(f"invalid bool byte {b}")
            return b == 1
        if name == "pubkey":
            return b58encode(r.take(32))
        if name == "string":
            raw = r.take(r.u32())
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise DecodeError(f"invalid utf-8 in string: {exc}") from None
        if name == "bytes":
            return r.take(r.u32()).hex()
        if name == "f32":
            return struct.unpack("<f", r.take(4))[0]
        if name == "f64":
            return struct.unpack("<d", r.take(8))[0]
        raise DecodeError(f"unsupported type {ty!r}")
    if not isinstance(ty, Mapping) or len(ty) != 1:
        raise DecodeError(f"unsupported type {ty!r}")
    (kind, arg), = ty.items()
    if kind == "vec":
        n = r.u32()
        if n > r.remaining:  # every element takes at least one byte
            raise DecodeError(f"vec length {n} exceeds remaining {r.remaining} bytes")
        return [decode_value(r, arg, types, depth + 1) for _ in range(n)]
    if kind == "option":
        tag = r.take(1)[0]
        if tag == 0:
            return None
        if tag == 1:
            return decode_value(r, arg, types, depth + 1)
        raise DecodeError(f"invalid option tag {tag}")
    if kind == "coption":
        tag = r.u32()
        if tag == 0:
            return None
        if tag == 1:
            return decode_value(r, arg, types, depth + 1)
        raise DecodeError(f"invalid coption tag {tag}")
    if kind == "array":
        inner, length = arg
        if not isinstance(length, int):
            raise DecodeError("generic array lengths are not supported")
        return [decode_value(r, inner, types, depth + 1) for _ in range(length)]
    if kind == "defined":
        name = defined_name(arg)
        if name not in types:
            raise DecodeError(f"unknown defined type {name!r}")
        return decode_typedef(r, types[name], types, depth + 1)
    raise DecodeError(f"unsupported type {ty!r}")


def decode_fields(r: Reader, fields: Any, types: Mapping[str, Any], depth: int = 0) -> Any:
    """Named fields -> dict, tuple fields -> list, none -> {}."""
    if not fields:
        return {}
    if all(isinstance(f, Mapping) and "name" in f for f in fields):
        return {f["name"]: decode_value(r, f["type"], types, depth) for f in fields}
    return [decode_value(r, f, types, depth) for f in fields]


def decode_typedef(r: Reader, typedef: Mapping[str, Any], types: Mapping[str, Any], depth: int = 0) -> Any:
    body = typedef.get("type", typedef)
    kind = body.get("kind")
    if kind == "struct":
        return decode_fields(r, body.get("fields"), types, depth)
    if kind == "enum":
        variants = body.get("variants") or []
        idx = r.take(1)[0]
        if idx >= len(variants):
            raise DecodeError(f"enum variant {idx} out of range ({len(variants)} variants)")
        variant = variants[idx]
        if not variant.get("fields"):
            return variant["name"]
        return {variant["name"]: decode_fields(r, variant["fields"], types, depth)}
    if kind == "type":
        return decode_value(r, body["alias"], types, depth)
    raise DecodeError(f"unsupported typedef kind {kind!r}")
