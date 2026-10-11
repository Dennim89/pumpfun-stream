"""Source -> bounded queue -> decoder -> dedup -> sinks, with optional getTransaction fetches."""
from __future__ import annotations

import asyncio
import sys
import time
from collections import Counter
from typing import Any, Iterable, TextIO

from .backoff import Clock, Sleep
from .decoder import DECODE_ERROR, UNKNOWN, Decoder, Payload, TxFormatError
from .records import Record, utc_iso
from .rpc import HttpRpc
from .sinks import Dedup
from .sources import Item, Source, TxItem

_SOURCE_DONE = object()  # the source has no more items
_END = object()          # ... and every pending getTransaction fetch has finished


class Pipeline:
    def __init__(self, decoder: Decoder, sinks: Iterable[Any], rpc: HttpRpc | None = None,
                 full_tx: bool = False, commitment: str = "confirmed", queue_size: int = 10_000,
                 fetch_queue_size: int = 1_000, fetch_workers: int = 4, fetch_null_retries: int = 3,
                 dedup_size: int = 200_000, diagnostics: bool = True, status_interval: float = 10.0,
                 status_stream: TextIO | None = None, wall_clock: Clock = time.time,
                 clock: Clock = time.monotonic, sleep: Sleep = asyncio.sleep):
        self.decoder = decoder
        self.sinks = list(sinks)
        self.rpc = rpc
        self.full_tx = full_tx
        self.commitment = "confirmed" if commitment == "processed" else commitment
        self.queue_size = queue_size
        self.fetch_queue_size = fetch_queue_size
        self.fetch_workers = fetch_workers
        self.fetch_null_retries = fetch_null_retries
        self.dedup = Dedup(dedup_size)
        self.fetched_sigs = Dedup(max(1000, fetch_queue_size * 4))
        self.diagnostics = diagnostics
        self.status_interval = status_interval
        self.status_stream = status_stream if status_stream is not None else sys.stderr
        self.wall_clock = wall_clock
        self.clock = clock
        self.sleep = sleep
        self.counts: Counter = Counter()
        self.source: Source | None = None
        self.queue: asyncio.Queue | None = None
        self.fetch_queue: asyncio.Queue | None = None
        self._last_status = (0.0, 0)
        self._warned_no_http = False

    # -- record handling --------------------------------------------------------------
    def _emit(self, payloads: list[Payload], signature: str, slot, block_time, source: str) -> None:
        received = utc_iso(self.wall_clock())
        for index, payload in enumerate(payloads):
            d = self.decoder.decode(payload)
            rec = Record(d.program, d.event, signature, slot, block_time, index, received, source,
                         d.fields, d.extra_bytes)
            if self.dedup.seen(rec.key):
                self.counts["duplicates"] += 1
                continue
            if d.event in (UNKNOWN, DECODE_ERROR):
                self.counts["unknown" if d.event == UNKNOWN else "decode_errors"] += 1
                if not self.diagnostics:
                    continue
            else:
                self.counts["events"] += 1
            for sink in self.sinks:
                sink.write(rec)

    def handle(self, item: Item) -> None:
        self.counts["items"] += 1
        if isinstance(item, TxItem):
            try:
                scan = self.decoder.scan_transaction(item.result)
            except (TxFormatError, ValueError, KeyError, TypeError, IndexError):
                self.counts["bad_items"] += 1
                return
            if scan.err is not None:
                self.counts["failed_tx"] += 1
                return
            if scan.truncated:
                self.counts["truncated_logs"] += 1
            if scan.signature is None:
                self.counts["bad_items"] += 1
                return
            self._emit(scan.payloads, scan.signature, scan.slot, scan.block_time, item.source)
            return
        if item.err is not None:
            self.counts["failed_tx"] += 1
            return
        scan = self.decoder.scan_logs(item.logs)
        if scan.truncated:
            self.counts["truncated_logs"] += 1
        self._emit(scan.payloads, item.signature, item.slot, item.block_time, item.source)
        if self.full_tx or scan.self_cpi or scan.truncated:
            self._request_fetch(item.signature)

    def _request_fetch(self, signature: str) -> None:
        if self.rpc is None or self.fetch_queue is None:
            self.counts["cpi_not_fetched"] += 1
            if not self._warned_no_http:
                self._warned_no_http = True
                self._say("note: some transactions may carry emit_cpi events that only the full transaction shows; "
                          "with run, pass --http to fetch them")
            return
        if self.fetched_sigs.seen(signature):
            return
        try:
            self.fetch_queue.put_nowait(signature)
        except asyncio.QueueFull:
            self.counts["fetch_dropped"] += 1

    async def _fetch_worker(self) -> None:
        assert self.fetch_queue is not None and self.queue is not None and self.rpc is not None
        while True:
            signature = await self.fetch_queue.get()
            try:
                result = None
                for attempt in range(self.fetch_null_retries + 1):
                    result = await self.rpc.get_transaction(signature, self.commitment)
                    if result is not None:
                        break
                    await self.sleep(0.5 * (2 ** attempt))  # not yet visible at this commitment
                if result is None:
                    self.counts["fetch_not_found"] += 1
                else:
                    self.counts["fetched"] += 1
                    await self.queue.put(TxItem(result, "rpc"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a worker must not die on one bad fetch
                self.counts["fetch_errors"] += 1
                self._say(f"getTransaction failed: {exc.__class__.__name__}: {exc}"[:300])
            finally:
                self.fetch_queue.task_done()

    # -- status -------------------------------------------------------------------------
    def _say(self, text: str) -> None:
        print(text, file=self.status_stream, flush=True)

    def status_line(self, prefix: str = "status", rate: bool = True) -> str:
        now = self.clock()
        last_t, last_n = self._last_status
        events = self.counts["events"]
        eps = (events - last_n) / (now - last_t) if now > last_t else 0.0
        self._last_status = (now, events)
        src_counts = getattr(self.source, "counts", Counter())
        q = self.queue.qsize() if self.queue else 0
        fq = self.fetch_queue.qsize() if self.fetch_queue else 0
        eps_part = f" eps={eps:.1f}" if rate else ""
        return (f"[{prefix}] events={events}{eps_part} reconnects={src_counts.get('reconnects', 0)} "
                f"decode_errors={self.counts['decode_errors']} unknown={self.counts['unknown']} "
                f"dup={self.counts['duplicates']} failed_tx={self.counts['failed_tx']} "
                f"queue={q}/{self.queue_size} fetch_queue={fq}/{self.fetch_queue_size} "
                f"dropped={self.counts['dropped'] + self.counts['fetch_dropped']}")

    async def _status_loop(self) -> None:
        while True:
            await self.sleep(self.status_interval)
            for sink in self.sinks:
                sink.flush()
            self._say(self.status_line())

    # -- main loop ------------------------------------------------------------------------
    async def _produce(self, source: Source) -> None:
        assert self.queue is not None
        async for item in source.items():
            if source.live:
                try:
                    self.queue.put_nowait(item)
                except asyncio.QueueFull:
                    self.counts["dropped"] += 1
            else:
                await self.queue.put(item)
        await self.queue.put(_SOURCE_DONE)

    async def _finish_fetches(self) -> None:
        assert self.queue is not None
        if self.fetch_queue is not None:
            await self.fetch_queue.join()
        await self.queue.put(_END)

    async def run(self, source: Source) -> Counter:
        self.source = source
        self.queue = asyncio.Queue(self.queue_size)
        self._last_status = (self.clock(), 0)
        tasks = []
        if self.rpc is not None:
            self.fetch_queue = asyncio.Queue(self.fetch_queue_size)
            tasks += [asyncio.create_task(self._fetch_worker()) for _ in range(self.fetch_workers)]
        if self.status_interval > 0:
            tasks.append(asyncio.create_task(self._status_loop()))
        producer = asyncio.create_task(self._produce(source))
        try:
            while True:
                if producer.done():
                    producer.result()  # re-raise a producer crash; otherwise drain up to _END
                    item = await self.queue.get()
                else:
                    get = asyncio.ensure_future(self.queue.get())
                    done, _ = await asyncio.wait({get, producer}, return_when=asyncio.FIRST_COMPLETED)
                    if get not in done:
                        get.cancel()
                        continue
                    item = get.result()
                if item is _SOURCE_DONE:
                    tasks.append(asyncio.create_task(self._finish_fetches()))
                    continue
                if item is _END:
                    break
                self.handle(item)
        finally:
            for task in tasks + [producer]:
                task.cancel()
            await asyncio.gather(*tasks, producer, return_exceptions=True)
            for sink in self.sinks:
                sink.flush()
        return self.counts
