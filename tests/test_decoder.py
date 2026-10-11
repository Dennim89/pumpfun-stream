import base64
import random

import pytest
from encoder import (cpi_instruction, encode_event, gen_event, gen_pubkey, invoke_logs, program_data_line,
                     self_cpi_logs, synthetic_signature, tx_result)
from synthetic_idl import IDL, PROGRAM_ID

from pumpfun_stream.decoder import DECODE_ERROR, UNKNOWN, Decoder, Payload
from pumpfun_stream.idl import EVENT_IX_TAG, load_idl

OTHER_PROGRAM = gen_pubkey(random.Random(2002))


@pytest.fixture
def decoder():
    return Decoder({"synthetic": load_idl(IDL)})


def small(rng, value=None):
    v = gen_event(IDL, "Small", rng)
    if value is not None:
        v["value"] = str(value)
    return v


def test_emit_event_from_logs(decoder):
    rng = random.Random(1)
    value = small(rng, 42)
    logs = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", value))])
    scan = decoder.scan_logs(logs)
    assert len(scan.payloads) == 1 and scan.payloads[0].via == "log"
    d = decoder.decode(scan.payloads[0])
    assert (d.program, d.event, d.fields, d.extra_bytes) == ("synthetic", "Small", value, 0)
    assert scan.self_cpi == set()


def test_program_data_is_attributed_to_the_running_program(decoder):
    rng = random.Random(2)
    ours = encode_event(IDL, "Small", small(rng, 1))
    foreign = encode_event(IDL, "Small", small(rng, 2))
    logs = (invoke_logs(OTHER_PROGRAM, [program_data_line(foreign)])
            + invoke_logs(PROGRAM_ID, invoke_logs(OTHER_PROGRAM, [program_data_line(foreign)], depth=2)
                          + [program_data_line(ours)]))
    scan = decoder.scan_logs(logs)
    assert [p.data for p in scan.payloads] == [ours]


def test_self_invocation_marks_possible_emit_cpi(decoder):
    logs = invoke_logs(PROGRAM_ID, self_cpi_logs(PROGRAM_ID))
    assert decoder.scan_logs(logs).self_cpi == {"synthetic"}


def test_emit_cpi_event_from_inner_instruction(decoder):
    rng = random.Random(3)
    sig = synthetic_signature(rng)
    v_log, v_cpi = small(rng, 10), small(rng, 11)
    logs = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", v_log))] + self_cpi_logs(PROGRAM_ID))
    result = tx_result(sig, [gen_pubkey(rng), PROGRAM_ID], rng, 77, logs,
                       [cpi_instruction(1, encode_event(IDL, "Small", v_cpi)),
                        {"programIdIndex": 1, "accounts": [], "data": "3Bxs4h24hBtQy9rw"}],  # not an event
                       block_time=1767225600)
    scan = decoder.scan_transaction(result)
    assert scan.signature == sig and scan.slot == 77 and scan.block_time == 1767225600
    assert [p.via for p in scan.payloads] == ["log", "cpi"]
    assert [decoder.decode(p).fields for p in scan.payloads] == [v_log, v_cpi]


def test_v0_transaction_with_loaded_program_address(decoder):
    rng = random.Random(4)
    sig = synthetic_signature(rng)
    payer = gen_pubkey(rng)
    value = small(rng, 5)
    # The program id is not a static key: it comes from an address lookup table (index 2).
    result = tx_result(sig, [payer, OTHER_PROGRAM], rng, 5, [], [cpi_instruction(2, encode_event(IDL, "Small", value))],
                       version0=True, loaded={"writable": [], "readonly": [PROGRAM_ID]})
    scan = decoder.scan_transaction(result)
    assert scan.signature == sig
    assert [decoder.decode(p).fields for p in scan.payloads] == [value]


def test_trailing_bytes_are_tolerated(decoder):
    raw = encode_event(IDL, "Small", small(random.Random(5), 3)) + b"\xaa\xbb\xcc"
    d = decoder.decode(Payload("synthetic", raw, "log"))
    assert d.event == "Small" and d.extra_bytes == 3


def test_unknown_discriminator_is_counted_per_program(decoder):
    raw = b"\x01" * 8 + b"payload"
    for _ in range(2):
        d = decoder.decode(Payload("synthetic", raw, "log"))
    assert d.event == UNKNOWN and d.fields["discriminator"] == "0101010101010101"
    assert decoder.counters.unknown == {"synthetic": 2}


def test_truncated_payload_is_a_decode_error_not_a_crash(decoder):
    raw = encode_event(IDL, "Small", small(random.Random(6), 3))[:20]
    d = decoder.decode(Payload("synthetic", raw, "log"))
    assert d.event == DECODE_ERROR and d.fields["event"] == "Small"
    assert decoder.counters.decode_errors == {"synthetic": 1}


def test_bad_base64_and_log_truncation(decoder):
    logs = invoke_logs(PROGRAM_ID, ["Program data: !!notbase64!!", "Log truncated"])
    scan = decoder.scan_logs(logs)
    assert scan.truncated
    assert decoder.decode(scan.payloads[0]).event == DECODE_ERROR


def test_decode_raw_strips_the_cpi_tag(decoder):
    raw = encode_event(IDL, "Small", small(random.Random(7), 8))
    assert decoder.decode_raw("synthetic", EVENT_IX_TAG + raw).event == "Small"
    assert decoder.decode_raw("synthetic", raw).fields["value"] == "8"
    assert base64.b64encode(raw)  # payloads are plain bytes
