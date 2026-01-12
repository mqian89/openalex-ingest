from __future__ import annotations

import asyncio
import itertools
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.openalex.org/works"


# =========================
# Helpers
# =========================
def chunk(iterable: Iterable[str], size: int) -> Iterator[List[str]]:
    """Yield lists of length `size` from `iterable`."""
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            return
        yield batch


def batch_fname(output_dir: str, batch_index: int) -> str:
    return os.path.join(output_dir, f"works_batch_{batch_index:04d}.parquet")


# =========================
# Soft throttling monitor
# =========================
@dataclass
class SoftThrottleMonitor:
    """
    Detect likely 'soft throttling' by comparing recent avg latency against a baseline.
    """

    lat_short: deque = deque(maxlen=8)
    lat_long: deque = deque(maxlen=40)
    last_warn_ts: float = 0.0

    slow_factor: float = 1.5
    min_baseline: float = 0.5
    warn_cooldown_s: float = 30.0

    def observe(self, dt: float) -> Optional[str]:
        self.lat_short.append(dt)
        self.lat_long.append(dt)

        if len(self.lat_long) < self.lat_long.maxlen or len(self.lat_short) < self.lat_short.maxlen:
            return None

        short_avg = sum(self.lat_short) / len(self.lat_short)
        long_avg = sum(self.lat_long) / len(self.lat_long)

        now = time.time()
        slowing = (long_avg >= self.min_baseline) and (short_avg >= self.slow_factor * long_avg)

        if slowing and (now - self.last_warn_ts) >= self.warn_cooldown_s:
            self.last_warn_ts = now
            return (
                f"Likely soft throttling: recent avg {short_avg:.2f}s vs baseline {long_avg:.2f}s "
                f"(x{short_avg/long_avg:.2f})"
            )

        return None


# =========================
# Async fetch functions
# =========================
async def fetch_page(
    client: httpx.AsyncClient,
    base_url: str,
    params: Dict[str, Any],
    *,
    semaphore: asyncio.Semaphore,
    monitor: SoftThrottleMonitor,
    max_attempts: int = 8,
) -> Dict[str, Any]:
    """
    Fetch one OpenAlex page with:
    - concurrency limiting via `semaphore` (only around the actual HTTP call)
    - retry/backoff on 429/503 and transient transport errors
    - soft-throttling detection based on recent vs baseline latency
    """
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_attempts + 1):
        t0 = time.time()
        try:
            async with semaphore:
                r = await client.get(base_url, params=params)

            dt = time.time() - t0

            # latency tracking + soft-throttle detection
            warn_msg = monitor.observe(dt)
            if warn_msg:
                logger.warning(warn_msg)

            logger.info(
                "openalex_response status=%s latency=%.2fs remaining=%s retry_after=%s",
                r.status_code,
                dt,
                r.headers.get("x-ratelimit-remaining"),
                r.headers.get("Retry-After"),
            )

            # retry on 429/503
            if r.status_code in (429, 503):
                retry_after = r.headers.get("Retry-After")
                base_wait = float(retry_after) if retry_after else min(60.0, 2.0**attempt)
                wait = base_wait + random.random()  # jitter

                logger.warning(
                    "HTTP %s from OpenAlex; backing off %.1fs (attempt %s/%s)",
                    r.status_code,
                    wait,
                    attempt,
                    max_attempts,
                )

                if attempt == max_attempts:
                    r.raise_for_status()

                await asyncio.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except (httpx.ReadError, httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:
            dt = time.time() - t0
            last_exc = e

            wait = min(60.0, 2.0**attempt) + random.random()
            logger.warning(
                "Transport error after %.2fs: %s — retrying in %.1fs (attempt %s/%s)",
                dt,
                type(e).__name__,
                wait,
                attempt,
                max_attempts,
            )

            if attempt == max_attempts:
                raise

            await asyncio.sleep(wait)
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError("fetch_page failed without raising a specific exception")


async def fetch_batch(
    client: httpx.AsyncClient,
    batch_ids: Sequence[str],
    batch_index: int,
    *,
    output_dir: str,
    base_url: str = DEFAULT_BASE_URL,
    per_page: int = 200,
    mailto: str,
    semaphore: asyncio.Semaphore,
    monitor: SoftThrottleMonitor,
    max_attempts: int = 8,
    skip_existing: bool = True,
) -> int:
    """
    Fetch all works for a batch of OpenAlex Author IDs and save to a parquet file.
    """
    os.makedirs(output_dir, exist_ok=True)
    fname = batch_fname(output_dir, batch_index)

    if skip_existing and os.path.exists(fname):
        logger.info("Skipping batch %s — file exists: %s", batch_index, fname)
        return batch_index

    logger.info("START batch %s (%s IDs)", batch_index, len(batch_ids))

    ids_str = "|".join(batch_ids)
    params: Dict[str, Any] = {
        "filter": f"authorships.author.id:{ids_str}",
        "per-page": per_page,
        "cursor": "*",
        "mailto": mailto,
    }

    works: List[Dict[str, Any]] = []

    while True:
        data = await fetch_page(
            client,
            base_url,
            params,
            semaphore=semaphore,
            monitor=monitor,
            max_attempts=max_attempts,
        )
        works.extend(data.get("results", []))

        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor

    # Identify IDs with / without works (best-effort; depends on returned authorships)
    ids_with_works = set()
    for w in works:
        for a in w.get("authorships", []) or []:
            author = a.get("author") or {}
            aid = author.get("id")
            if not aid:
                continue
            aid = aid.split("/")[-1]
            if aid in batch_ids:
                ids_with_works.add(aid)

    ids_without = sorted(set(batch_ids) - ids_with_works)
    if ids_without:
        logger.warning("Batch %s: IDs with NO works → %s", batch_index, ", ".join(ids_without))
    else:
        logger.info("Batch %s: all IDs returned works", batch_index)

    # Save parquet
    if works:
        df_batch = pd.json_normalize(works)
        df_batch.to_parquet(fname, index=False)
        logger.info("Saved batch %s: %s works → %s", batch_index, len(df_batch), fname)
    else:
        logger.info("Batch %s: no works returned (nothing saved)", batch_index)

    logger.info("DONE batch %s", batch_index)
    return batch_index


async def main(
    author_ids: Sequence[str],
    *,
    output_dir: str,
    mailto: str,
    batch_size: int = 10,
    per_page: int = 200,
    max_concurrent_http: int = 10,
    max_concurrent_batches: int = 5,
    timeout_s: float = 60.0,
    base_url: str = DEFAULT_BASE_URL,
    skip_existing: bool = True,
    max_attempts: int = 8,
    deterministic_batches: bool = False,
) -> None:
    """
    Orchestrate batch downloading of OpenAlex works for a list of author IDs.

    Parameters
    ----------
    author_ids:
        Sequence of OpenAlex Author IDs (e.g., "A1234567890") — no URL prefix needed.
    output_dir:
        Directory to save parquet batch files.
    mailto:
        Email address to include in requests (OpenAlex best practice).
    deterministic_batches:
        If True, sorts and de-dupes IDs to keep batch membership stable across runs.
    """
    if not mailto:
        raise ValueError("mailto must be provided (OpenAlex recommends setting mailto).")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if per_page <= 0 or per_page > 200:
        raise ValueError("per_page must be in 1..200")
    if max_concurrent_http <= 0:
        raise ValueError("max_concurrent_http must be > 0")
    if max_concurrent_batches <= 0:
        raise ValueError("max_concurrent_batches must be > 0")

    os.makedirs(output_dir, exist_ok=True)

    ids = list(author_ids)
    if deterministic_batches:
        ids = sorted(set(x.strip() for x in ids if str(x).strip()))
    else:
        ids = [str(x).strip() for x in ids if str(x).strip()]

    if not ids:
        logger.info("No author IDs provided — nothing to fetch.")
        return

    # Concurrency primitives belong to the run, not module globals
    semaphore = asyncio.Semaphore(max_concurrent_http)
    monitor = SoftThrottleMonitor()

    queue: asyncio.Queue[tuple[int, List[str]]] = asyncio.Queue()

    for batch_index, batch in enumerate(chunk(ids, batch_size), start=1):
        if skip_existing and os.path.exists(batch_fname(output_dir, batch_index)):
            logger.info("Skipping batch %s — file already exists", batch_index)
            continue
        queue.put_nowait((batch_index, batch))

    if queue.empty():
        logger.info("Nothing to fetch — all batches already processed.")
        return

    async with httpx.AsyncClient(timeout=timeout_s) as client:

        async def worker(worker_id: int) -> None:
            while True:
                try:
                    batch_index, batch = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await fetch_batch(
                        client,
                        batch,
                        batch_index,
                        output_dir=output_dir,
                        base_url=base_url,
                        per_page=per_page,
                        mailto=mailto,
                        semaphore=semaphore,
                        monitor=monitor,
                        max_attempts=max_attempts,
                        skip_existing=skip_existing,
                    )
                except Exception:
                    logger.exception("Worker %s: batch %s failed", worker_id, batch_index)
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker(i)) for i in range(max_concurrent_batches)]
        await queue.join()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
