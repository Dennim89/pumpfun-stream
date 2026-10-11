"""Command line: run, replay, decode, stats."""
from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import signal
import sys
from pathlib import Path

from . import PROGRAM_IDS, __version__
from .backoff import RateLimiter
from .base58 import b58decode
from .decoder import Decoder
from .idl import Idl, default_idl_path, load_idl
from .pipeline import Pipeline
from .rpc import HttpRpc
from .sinks import JsonlSink, SqliteSink
from .sources import ReplaySource
from .stats import render, summarise
from .ws import WsLogsSource

ENV_PREFIX = "PUMPFUN_STREAM_"


def env(name: str, default=None):
    return os.environ.get(ENV_PREFIX + name, default)


def load_idls(programs: list[str], overrides: list[str]) -> dict[str, Idl]:
    paths = {}
    for item in overrides or []:
        name, sep, path = item.partition("=")
        if not sep or not name or not path:
            raise SystemExit(f"--idl expects NAME=PATH, got {item!r}")
        paths[name] = Path(path)
    idls = {}
    for name in programs:
        path = paths.get(name, default_idl_path(name))
        if not path.is_file():
            raise SystemExit(f"no IDL for {name!r} at {path}; pass --idl {name}=path/to/{name}.json")
        idl = load_idl(path, name=name)
        if idl.address is None:
            idl.address = PROGRAM_IDS.get(name)
        if idl.address is None:
            raise SystemExit(f"IDL {path} has no program address")
        for w in idl.warnings:
            print(f"warning: {w}", file=sys.stderr)
        idls[name] = idl
    return idls


def build_sinks(args, idls: dict[str, Idl]):
    sinks = []
    if args.out:
        sinks.append(JsonlSink(args.out))
    if args.sqlite:
        meta = {"package_version": __version__}
        for name, idl in idls.items():
            meta[f"idl_sha256:{name}"] = idl.sha256
            meta[f"idl_version:{name}"] = str(idl.version)
            meta[f"program_id:{name}"] = idl.address or ""
        sinks.append(SqliteSink(args.sqlite, meta))
    if not sinks:
        raise SystemExit("nothing to write: give --out and/or --sqlite")
    return sinks


def _programs(value: str) -> list[str]:
    names = [p.strip() for p in value.split(",") if p.strip()]
    if not names:
        raise argparse.ArgumentTypeError("empty program list")
    return names


def _add_common(p: argparse.ArgumentParser, outputs: bool = True) -> None:
    p.add_argument("--programs", type=_programs, default=_programs(env("PROGRAMS", "pump,pump_amm")),
                   help="comma-separated program names (default: pump,pump_amm)")
    p.add_argument("--idl", action="append", metavar="NAME=PATH",
                   help="use this IDL file for a program (repeatable)")
    if outputs:
        p.add_argument("--out", default=env("OUT", "events.jsonl"),
                       help="JSONL output file, '-' for stdout, '' to disable (default: events.jsonl)")
        p.add_argument("--sqlite", default=env("SQLITE"), help="also write to this SQLite file")
        p.add_argument("--no-diagnostics", action="store_true",
                       help="do not write _unknown / _decode_error records (they are still counted)")
        p.add_argument("--status-interval", type=float, default=float(env("STATUS_INTERVAL", "10")),
                       help="seconds between status lines on stderr, 0 to disable (default: 10)")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="pumpfun-stream",
                                 description="Stream and decode pump.fun / PumpSwap Anchor events.")
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="stream live events over an RPC WebSocket")
    run.add_argument("--ws", default=env("WS_URL"), help="RPC WebSocket URL (env PUMPFUN_STREAM_WS_URL)")
    run.add_argument("--http", default=env("HTTP_URL"),
                     help="RPC HTTP URL for getTransaction (env PUMPFUN_STREAM_HTTP_URL)")
    run.add_argument("--full-tx", action="store_true",
                     help="fetch every transaction with getTransaction, not only those that may carry emit_cpi events")
    run.add_argument("--commitment", default=env("COMMITMENT", "confirmed"),
                     choices=["processed", "confirmed", "finalized"])
    run.add_argument("--rps", type=float, default=float(env("RPS", "5")),
                     help="max getTransaction requests per second, 0 = no limit (default: 5)")
    run.add_argument("--max-tx-version", type=int, default=int(env("MAX_TX_VERSION", "1")),
                     help="maxSupportedTransactionVersion for getTransaction (default: 1)")
    run.add_argument("--fetch-workers", type=int, default=int(env("FETCH_WORKERS", "4")))
    run.add_argument("--queue-size", type=int, default=int(env("QUEUE_SIZE", "10000")))
    run.add_argument("--fetch-queue-size", type=int, default=int(env("FETCH_QUEUE_SIZE", "1000")))
    run.add_argument("--idle-timeout", type=float, default=float(env("IDLE_TIMEOUT", "120")),
                     help="reconnect when nothing arrives for this many seconds (default: 120)")
    run.add_argument("--dedup-size", type=int, default=int(env("DEDUP_SIZE", "200000")))
    _add_common(run)

    rep = sub.add_parser("replay", help="decode a JSONL file of saved logs or transactions")
    rep.add_argument("file")
    _add_common(rep)

    dec = sub.add_parser("decode", help="decode one event payload")
    dec.add_argument("payload", help="base64 (as in 'Program data:') or base58 with --base58")
    dec.add_argument("--program", default="pump")
    dec.add_argument("--base58", action="store_true", help="payload is base58 (inner instruction data)")
    dec.add_argument("--idl", action="append", metavar="NAME=PATH")

    st = sub.add_parser("stats", help="summarise a JSONL output file")
    st.add_argument("file")
    return ap


async def _run_async(pipeline: Pipeline, source) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError):  # Windows
            pass
    try:
        await pipeline.run(source)
    except asyncio.CancelledError:
        pass


def cmd_run(args) -> int:
    if not args.ws:
        raise SystemExit("--ws (or PUMPFUN_STREAM_WS_URL) is required")
    idls = load_idls(args.programs, args.idl)
    sinks = build_sinks(args, idls)
    rpc = HttpRpc(args.http, RateLimiter(args.rps), max_tx_version=args.max_tx_version) if args.http else None
    if args.full_tx and rpc is None:
        raise SystemExit("--full-tx needs --http")
    log = lambda msg: print(msg, file=sys.stderr, flush=True)  # noqa: E731
    source = WsLogsSource(args.ws, {n: i.address for n, i in idls.items()}, args.commitment,
                          idle_timeout=args.idle_timeout, log=log)
    pipeline = Pipeline(Decoder(idls), sinks, rpc=rpc, full_tx=args.full_tx, commitment=args.commitment,
                        queue_size=args.queue_size, fetch_queue_size=args.fetch_queue_size,
                        fetch_workers=args.fetch_workers, dedup_size=args.dedup_size,
                        diagnostics=not args.no_diagnostics, status_interval=args.status_interval)
    try:
        asyncio.run(_run_async(pipeline, source))
    except KeyboardInterrupt:
        pass
    finally:
        for s in sinks:
            s.close()
        print(pipeline.status_line("stopped"), file=sys.stderr)
    return 0


def cmd_replay(args) -> int:
    idls = load_idls(args.programs, args.idl)
    sinks = build_sinks(args, idls)
    source = ReplaySource(args.file)
    pipeline = Pipeline(Decoder(idls), sinks, diagnostics=not args.no_diagnostics,
                        status_interval=args.status_interval)
    try:
        asyncio.run(pipeline.run(source))
    finally:
        for s in sinks:
            s.close()
    line = pipeline.status_line("done", rate=False)
    if source.bad_lines:
        line += f" unreadable_lines={source.bad_lines}"
    print(line, file=sys.stderr)
    return 0


def cmd_decode(args) -> int:
    idls = load_idls([args.program], args.idl)
    try:
        data = b58decode(args.payload) if args.base58 else base64.b64decode(args.payload, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise SystemExit(f"cannot read payload: {exc}")
    d = Decoder(idls).decode_raw(args.program, data)
    print(json.dumps({"program": d.program, "event": d.event, "fields": d.fields,
                      "extra_bytes": d.extra_bytes}, indent=2, ensure_ascii=False))
    return 0 if not d.event.startswith("_") else 1


def cmd_stats(args) -> int:
    with open(args.file, encoding="utf-8") as fh:
        print(render(summarise(fh)))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return {"run": cmd_run, "replay": cmd_replay, "decode": cmd_decode, "stats": cmd_stats}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
