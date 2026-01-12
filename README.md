# OpenAlex Ingest

Async ingestion utility to download OpenAlex **Works** metadata for a curated list of OpenAlex **Author IDs**, using parallelized batching, retries/backoff, and restartable output.

## What it does

* Reads a list of OpenAlex Author IDs from `inputs/author_ids.csv` (or `inputs/author_ids.txt`)
* Queries the OpenAlex Works endpoint using a batched author filter
* Downloads results using cursor pagination
* Writes output to disk in a restartable way (skips existing batches)

**Output format:** JSON Lines (`.jsonl`)
Each line is one OpenAlex `work` JSON object exactly as returned in `results`.

## Repository layout

* `src/openalex_ingest/ingest.py` — ingestion library (async)
* `run_ingest.py` — simple runner script (no CLI)
* `config.example.toml` — example configuration (copy to `config.toml`)
* `inputs/author_ids.csv` — your author IDs (not committed; create locally)
* `output/` (default) — output folder (not committed)

## Requirements

* Python 3.10+ recommended
* Dependencies (installed via `pyproject.toml`):

  * `httpx`
  * `pandas` *(only if used elsewhere; not required for JSONL output)*
  * `pyarrow` *(only required if using Parquet output)*

## Install

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows
```

Install in editable mode:

```bash
pip install -U pip
pip install -e .
```

## Configure

Copy the example config:

```bash
cp config.example.toml config.toml
```

Edit `config.toml` and set at least:

* `email` — contact email for OpenAlex `mailto` (recommended)
* `output_dir` — where output files are written

Example:

```toml
email = "your.email@example.com"
output_dir = "output"
batch_size = 10
max_concurrent_http = 10
max_concurrent_batches = 5
```

You can also override some settings via environment variables:

* `OPENALEX_EMAIL`
* `OUTPUT_DIR`
* `LOG_LEVEL`
* `BATCH_SIZE`, `PER_PAGE`, `MAX_CONCURRENT_HTTP`, `MAX_CONCURRENT_BATCHES`, `TIMEOUT_S`, `MAX_ATTEMPTS`

## Provide author IDs

### Option A: CSV (recommended)

Create: `inputs/author_ids.csv`

```csv
openalex_author_id
A5081428881
A5052026249
A5103572321
```

### Option B: TXT

Create: `inputs/author_ids.txt`

```
A5081428881
A5052026249
A5103572321
```

## Run ingestion

From the repo root:

```bash
python run_ingest.py
```

### Output

By default, output files look like:

* `output/works_batch_0001.jsonl`
* `output/works_batch_0002.jsonl`
* …

Each file contains newline-delimited JSON objects (one work per line).

## Notes on batching

`batch_size` controls how many author IDs are queried in a single request.

* If `batch_size > 1`, one batch file will include works across multiple authors in that batch.
* If you want one output file per author ID, set:

```toml
batch_size = 1
```

## Restart behavior

The ingestion run is restartable.

* If an output file for a given batch already exists, that batch is skipped (configurable via `skip_existing`).

## Troubleshooting

### “missing required column 'openalex_author_id'”

Your `inputs/author_ids.csv` is missing the header row. The first line must be:

```csv
openalex_author_id
```

### Parquet / engine errors

If you switch output back to Parquet, you’ll need an engine:

```bash
pip install pyarrow
```

### “mailto must be provided”

Set `email` in `config.toml` or export:

```bash
export OPENALEX_EMAIL="you@domain.com"
```

## License

Add a license (e.g., MIT) if you plan to reuse and share this repo.
