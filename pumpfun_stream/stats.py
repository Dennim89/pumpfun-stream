"""Summarise a JSONL file written by `run` or `replay`."""
from __future__ import annotations

import json
from collections import Counter
from typing import TextIO

from .decoder import DECODE_ERROR, UNKNOWN


def summarise(fh: TextIO) -> dict:
    events: Counter = Counter()
    unknown: Counter = Counter()
    errors: Counter = Counter()
    extra = 0
    bad = 0
    signatures = set()
    slots = []
    for line in fh:
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            program, event = rec["program"], rec["event"]
        except (ValueError, KeyError, TypeError):
            bad += 1
            continue
        if event == UNKNOWN:
            unknown[(program, (rec.get("fields") or {}).get("discriminator"))] += 1
        elif event == DECODE_ERROR:
            errors[(program, (rec.get("fields") or {}).get("event"))] += 1
        else:
            events[(program, event)] += 1
            signatures.add(rec.get("signature"))
            if isinstance(rec.get("slot"), int):
                slots.append(rec["slot"])
            if rec.get("extra_bytes"):
                extra += 1
    return {
        "events": events, "unknown": unknown, "decode_errors": errors, "with_extra_bytes": extra,
        "bad_lines": bad, "signatures": len(signatures),
        "slot_range": (min(slots), max(slots)) if slots else None,
    }


def render(s: dict) -> str:
    out = [f"events: {sum(s['events'].values())} in {s['signatures']} transactions"]
    if s["slot_range"]:
        out.append(f"slots: {s['slot_range'][0]}..{s['slot_range'][1]}")
    width = max([len(f"{p} {e}") for p, e in s["events"]] + [10])
    for (program, event), n in sorted(s["events"].items(), key=lambda kv: (-kv[1], kv[0])):
        out.append(f"  {(program + ' ' + event).ljust(width)}  {n}")
    out.append(f"records with extra trailing bytes: {s['with_extra_bytes']}")
    out.append(f"unknown discriminators: {sum(s['unknown'].values())}")
    for (program, disc), n in sorted(s["unknown"].items(), key=lambda kv: str(kv[0])):
        out.append(f"  {program} {disc}  {n}")
    out.append(f"decode errors: {sum(s['decode_errors'].values())}")
    for (program, event), n in sorted(s["decode_errors"].items(), key=lambda kv: str(kv[0])):
        out.append(f"  {program} {event or '?'}  {n}")
    if s["bad_lines"]:
        out.append(f"unreadable lines: {s['bad_lines']}")
    return "\n".join(out)
