from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import List, Optional

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # Python <3.11 (optional dependency)

from openalex_ingest.ingest import main


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = REPO_ROOT / "config.toml"
FALLBACK_CONFIG = REPO_ROOT / "config.example.toml"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_toml(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def load_author_ids(
    inputs_dir: Path,
    *,
    csv_filename: str = "author_ids.csv",
    txt_filename: str = "author_ids.txt",
    csv_column: str = "openalex_author_id",
) -> List[str]:
    """
    Load author IDs from either:
      - inputs/author_ids.csv with a column `openalex_author_id` (default), or
      - inputs/author_ids.txt with one ID per line.

    Returns a de-duplicated list in original order.
    """
    csv_path = inputs_dir / csv_filename
    txt_path = inputs_dir / txt_filename

    ids: List[str] = []

    if csv_path.exists():
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"{csv_path} has no header row.")
            if csv_column not in reader.fieldnames:
                raise ValueError(
                    f"{csv_path} is missing required column '{csv_column}'. "
                    f"Found columns: {reader.fieldnames}"
                )
            for row in reader:
                val = (row.get(csv_column) or "").strip()
                if val:
                    ids.append(val)

    elif txt_path.exists():
        with txt_path.open("r", encoding="utf-8") as f:
            for line in f:
                val = line.strip()
                if val and not val.startswith("#"):
                    ids.append(val)

    else:
        raise FileNotFoundError(
            f"Could not find {csv_path} or {txt_path}. "
            f"Create one of them with your OpenAlex author IDs."
        )

    # De-dupe while preserving order
    seen = set()
    deduped: List[str] = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            deduped.append(x)

    return deduped


def resolve_config_path() -> Path:
    if DEFAULT_CONFIG.exists():
        return DEFAULT_CONFIG
    if FALLBACK_CONFIG.exists():
        return FALLBACK_CONFIG
    raise FileNotFoundError(
        f"Expected {DEFAULT_CONFIG} or {FALLBACK_CONFIG} to exist."
    )


def get(d: dict, key: str, default=None):
    return d.get(key, default)


def main_sync() -> None:
    config_path = resolve_config_path()
    cfg = load_toml(config_path)

    # Allow env overrides for common settings (optional, DevOps-friendly)
    mailto = os.getenv("OPENALEX_EMAIL", get(cfg, "email", "")).strip()
    output_dir = os.getenv("OUTPUT_DIR", get(cfg, "output_dir", "batch_store_works_api"))
    log_level = os.getenv("LOG_LEVEL", get(cfg, "log_level", "INFO"))

    batch_size = int(os.getenv("BATCH_SIZE", str(get(cfg, "batch_size", 10))))
    per_page = int(os.getenv("PER_PAGE", str(get(cfg, "per_page", 200))))
    max_concurrent_http = int(os.getenv("MAX_CONCURRENT_HTTP", str(get(cfg, "max_concurrent_http", 10))))
    max_concurrent_batches = int(os.getenv("MAX_CONCURRENT_BATCHES", str(get(cfg, "max_concurrent_batches", 5))))
    timeout_s = float(os.getenv("TIMEOUT_S", str(get(cfg, "timeout_s", 60.0))))

    deterministic_batches = bool(get(cfg, "deterministic_batches", False))
    skip_existing = bool(get(cfg, "skip_existing", True))
    max_attempts = int(os.getenv("MAX_ATTEMPTS", str(get(cfg, "max_attempts", 8))))

    setup_logging(log_level)

    logger = logging.getLogger("run_ingest")
    logger.info("Using config: %s", config_path)

    inputs_dir = REPO_ROOT / "inputs"
    author_ids = load_author_ids(inputs_dir)

    logger.info("Loaded %d unique author IDs", len(author_ids))
    logger.info("Output dir: %s", output_dir)

    asyncio.run(
        main(
            author_ids,
            output_dir=output_dir,
            mailto=mailto,
            batch_size=batch_size,
            per_page=per_page,
            max_concurrent_http=max_concurrent_http,
            max_concurrent_batches=max_concurrent_batches,
            timeout_s=timeout_s,
            skip_existing=skip_existing,
            max_attempts=max_attempts,
            deterministic_batches=deterministic_batches,
        )
    )


if __name__ == "__main__":
    main_sync()
