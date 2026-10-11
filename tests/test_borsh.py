import hashlib
import random
import zlib

import pytest
from encoder import encode_event, encode_value, gen_event, gen_value, idl_types
from synthetic_idl import IDL, LEGACY_IDL

from pumpfun_stream.base58 import b58decode, b58encode
from pumpfun_stream.borsh import INT_SIZES, DecodeError, Reader, decode_value
from pumpfun_stream.idl import EVENT_IX_TAG, event_discriminator, load_idl

TYPES = idl_types(IDL)


def test_discriminator_is_sha256_of_event_name():
    assert event_discriminator("TradeEvent") == hashlib.sha256(b"event:TradeEvent").digest()[:8]
    assert len(event_discriminator("X")) == 8


def test_event_ix_tag_is_anchor_tag_little_endian():
    assert EVENT_IX_TAG == bytes.fromhex("e445a52e51cb9a1d")
    assert EVENT_IX_TAG == hashlib.sha256(b"anchor:event").digest()[:8][::-1]


def test_base58_round_trip_and_leading_zeros():
    rng = random.Random(3)
    for n in (0, 1, 31, 32, 64):
        for lead in (0, 1, 5):
            raw = bytes(lead) + rng.randbytes(n)
            assert b58decode(b58encode(raw)) == raw
    assert b58encode(bytes(32)) == "1" * 32
    with pytest.raises(ValueError):
        b58decode("0OIl")


SCALARS = sorted(INT_SIZES) + ["bool", "pubkey", "publicKey", "string", "bytes", "f32", "f64"]


@pytest.mark.parametrize("ty", SCALARS)
def test_scalar_round_trip(ty):
    rng = random.Random(zlib.crc32(ty.encode()))
    for _ in range(50):
        value = gen_value(ty, rng, TYPES)
        raw = encode_value(ty, value, TYPES)
        r = Reader(raw)
        assert decode_value(r, ty, TYPES) == value
        assert r.remaining == 0


@pytest.mark.parametrize("ty,lo,hi", [
    ("u64", 0, 2**64 - 1), ("i64", -(2**63), 2**63 - 1),
    ("u128", 0, 2**128 - 1), ("i128", -(2**127), 2**127 - 1),
])
def test_wide_integers_are_decimal_strings_at_the_limits(ty, lo, hi):
    for v in (lo, hi, -1 if lo else 1):
        raw = encode_value(ty, v, TYPES)
        assert decode_value(Reader(raw), ty, TYPES) == str(v)


def test_signed_values_stay_negative():
    raw = encode_event(IDL, "Signed", {"reserve": "-5", "big": str(-(2**100))})
    idl = load_idl(IDL)
    _, fields, extra = idl.decode_event(raw)
    assert fields == {"reserve": "-5", "big": str(-(2**100))}
    assert extra == 0


@pytest.mark.parametrize("seed", range(40))
def test_all_types_event_round_trip(seed):
    rng = random.Random(seed)
    value = gen_event(IDL, "AllTypes", rng)
    raw = encode_event(IDL, "AllTypes", value)
    ev, fields, extra = load_idl(IDL).decode_event(raw)
    assert ev.name == "AllTypes"
    assert fields == value
    assert extra == 0


def test_enum_variants_and_tuple_structs():
    idl = load_idl(IDL)
    base = gen_event(IDL, "AllTypes", random.Random(0))
    for variant in ("Unit", {"Named": {"x": -7, "label": "hi"}}, {"Tuple": [3, True]}):
        value = dict(base, a_enum=variant, a_tuple=[255, -128])
        _, fields, _ = idl.decode_event(encode_event(IDL, "AllTypes", value))
        assert fields["a_enum"] == variant
        assert fields["a_tuple"] == [255, -128]


def test_legacy_idl_format():
    idl = load_idl(LEGACY_IDL)
    who = b58encode(random.Random(5).randbytes(32))
    value = {"who": who, "inner": {"x": 9, "owner": None}, "amount": "123"}
    _, fields, extra = idl.decode_event(encode_event(LEGACY_IDL, "OldStyle", value))
    assert fields == value and extra == 0
    assert idl.address == IDL["address"]


@pytest.mark.parametrize("ty,raw,msg", [
    ("u64", b"\x01\x02", "unexpected end"),
    ("bool", b"\x02", "invalid bool"),
    ({"option": "u8"}, b"\x05\x00", "invalid option"),
    ({"vec": "u8"}, (1000).to_bytes(4, "little") + b"\x00", "exceeds remaining"),
    ("string", (3).to_bytes(4, "little") + b"\xff\xfe\xfd", "utf-8"),
    ({"defined": {"name": "Kind"}}, b"\x09", "out of range"),
    ({"defined": {"name": "Missing"}}, b"\x00", "unknown defined"),
    ("u512", b"\x00" * 64, "unsupported"),
])
def test_bad_input_raises_decode_error(ty, raw, msg):
    with pytest.raises(DecodeError, match=msg):
        decode_value(Reader(raw), ty, TYPES)
