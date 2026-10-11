"""A small synthetic IDL that uses every type the decoder supports."""
import random

from pumpfun_stream.base58 import b58encode

PROGRAM_ID = b58encode(random.Random(1001).randbytes(32))

ALL_FIELDS = [
    {"name": "a_u8", "type": "u8"}, {"name": "a_u16", "type": "u16"}, {"name": "a_u32", "type": "u32"},
    {"name": "a_u64", "type": "u64"}, {"name": "a_u128", "type": "u128"},
    {"name": "a_i8", "type": "i8"}, {"name": "a_i16", "type": "i16"}, {"name": "a_i32", "type": "i32"},
    {"name": "a_i64", "type": "i64"}, {"name": "a_i128", "type": "i128"},
    {"name": "a_f32", "type": "f32"}, {"name": "a_f64", "type": "f64"},
    {"name": "a_bool", "type": "bool"}, {"name": "a_pubkey", "type": "pubkey"},
    {"name": "a_string", "type": "string"}, {"name": "a_bytes", "type": "bytes"},
    {"name": "a_vec", "type": {"vec": "u32"}},
    {"name": "a_vec_vec", "type": {"vec": {"vec": "string"}}},
    {"name": "a_option", "type": {"option": "u64"}},
    {"name": "a_option_struct", "type": {"option": {"defined": {"name": "Inner"}}}},
    {"name": "a_coption", "type": {"coption": "u16"}},
    {"name": "a_array", "type": {"array": ["u8", 4]}},
    {"name": "a_array_struct", "type": {"array": [{"defined": {"name": "Inner"}}, 2]}},
    {"name": "a_struct", "type": {"defined": {"name": "Inner"}}},
    {"name": "a_enum", "type": {"defined": {"name": "Kind"}}},
    {"name": "a_tuple", "type": {"defined": {"name": "Pair"}}},
    {"name": "a_alias", "type": {"defined": {"name": "Lamports"}}},
]

IDL = {
    "address": PROGRAM_ID,
    "metadata": {"name": "synthetic", "version": "0.0.1", "spec": "0.1.0"},
    "events": [{"name": "AllTypes"}, {"name": "Small"}, {"name": "Signed"}],
    "types": [
        {"name": "AllTypes", "type": {"kind": "struct", "fields": ALL_FIELDS}},
        {"name": "Small", "type": {"kind": "struct", "fields": [
            {"name": "value", "type": "u64"}, {"name": "who", "type": "pubkey"}]}},
        {"name": "Signed", "type": {"kind": "struct", "fields": [
            {"name": "reserve", "type": "i64"}, {"name": "big", "type": "i128"}]}},
        {"name": "Inner", "type": {"kind": "struct", "fields": [
            {"name": "x", "type": "u16"}, {"name": "owner", "type": {"option": "pubkey"}}]}},
        {"name": "Kind", "type": {"kind": "enum", "variants": [
            {"name": "Unit"},
            {"name": "Named", "fields": [{"name": "x", "type": "i32"}, {"name": "label", "type": "string"}]},
            {"name": "Tuple", "fields": ["u8", "bool"]}]}},
        {"name": "Pair", "type": {"kind": "struct", "fields": ["u8", "i8"]}},
        {"name": "Lamports", "type": {"kind": "type", "alias": "u64"}},
    ],
}

# The pre-0.30 layout: fields inline in the event, publicKey, defined as a plain string.
LEGACY_IDL = {
    "name": "legacy_synthetic",
    "version": "0.0.1",
    "metadata": {"address": PROGRAM_ID},
    "events": [{"name": "OldStyle", "fields": [
        {"name": "who", "type": "publicKey", "index": False},
        {"name": "inner", "type": {"defined": "Inner"}, "index": False},
        {"name": "amount", "type": "u64", "index": False}]}],
    "types": [{"name": "Inner", "type": {"kind": "struct", "fields": [
        {"name": "x", "type": "u16"}, {"name": "owner", "type": {"option": "publicKey"}}]}}],
}
