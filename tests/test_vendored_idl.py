"""Every event in the vendored pump.fun / PumpSwap IDLs round-trips through the encoder in tests/."""
import json
import random

import pytest
from encoder import encode_event, gen_event

from pumpfun_stream import PROGRAM_IDS
from pumpfun_stream.idl import default_idl_path, event_discriminator, load_idl

PROGRAMS = [p for p in ("pump", "pump_amm") if default_idl_path(p).is_file()]
pytestmark = pytest.mark.skipif(not PROGRAMS, reason="vendored IDL files are not present")


@pytest.mark.parametrize("program", PROGRAMS)
def test_vendored_idl_address(program):
    assert load_idl(default_idl_path(program)).address == PROGRAM_IDS[program]


@pytest.mark.parametrize("program", PROGRAMS)
def test_every_vendored_event_round_trips(program):
    doc = json.loads(default_idl_path(program).read_text())
    idl = load_idl(default_idl_path(program))
    rng = random.Random(11)
    for ev in doc["events"]:
        for _ in range(3):
            value = gen_event(doc, ev["name"], rng)
            got, fields, extra = idl.decode_event(encode_event(doc, ev["name"], value))
            assert got.name == ev["name"]
            assert fields == value
            assert extra == 0


@pytest.mark.parametrize("program", PROGRAMS)
def test_computed_discriminators_match_the_vendored_idl(program):
    doc = json.loads(default_idl_path(program).read_text())
    assert doc["events"]
    for ev in doc["events"]:
        assert event_discriminator(ev["name"]) == bytes(ev["discriminator"]), ev["name"]
    assert load_idl(default_idl_path(program)).warnings == []
