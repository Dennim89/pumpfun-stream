"""Keep only what the decoder needs from an Anchor IDL: address, metadata, events, types.

    python scripts/trim_idl.py full_pump.json pumpfun_stream/idl/pump.json

The decoder also reads full IDL files; trimming only keeps the repository small.
"""
import json
import sys

KEEP = ("address", "metadata", "events", "types")


def main(src: str, dst: str) -> None:
    with open(src, encoding="utf-8") as fh:
        doc = json.load(fh)
    out = {k: doc[k] for k in KEEP if k in doc}
    with open(dst, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
        fh.write("\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
