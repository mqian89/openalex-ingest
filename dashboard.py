from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st


st.set_page_config(page_title="OpenAlex Ingest Dashboard", layout="wide")

st.title("OpenAlex Ingest Dashboard")

repo_root = Path(__file__).resolve().parent
output_dir = st.sidebar.text_input("Output directory", value="output")
out = (repo_root / output_dir).resolve()

st.sidebar.write(f"Reading: `{out}`")

pattern = "works_batch_*.jsonl"
files = sorted(out.glob(pattern))

if not out.exists():
    st.error(f"Output directory does not exist: {out}")
    st.stop()

st.metric("Batch files", len(files))

def count_lines(p: Path) -> int:
    # JSONL: one object per line
    with p.open("rb") as f:
        return sum(1 for _ in f)

if files:
    # show recent batches
    rows = []
    for p in files[-30:]:
        rows.append(
            {
                "file": p.name,
                "modified": datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds"),
                "size_mb": round(p.stat().st_size / (1024 * 1024), 2),
                "works": count_lines(p),
            }
        )
    df = pd.DataFrame(rows)
    st.subheader("Recent batches")
    st.dataframe(df, use_container_width=True)

    st.subheader("Total works (approx)")
    total_works = sum(count_lines(p) for p in files)
    st.metric("Works (JSONL lines)", total_works)

    st.subheader("Peek a sample work")
    pick = st.selectbox("Choose a batch", [p.name for p in files], index=len(files) - 1)
    chosen = out / pick

    # read first non-empty line
    sample = None
    with chosen.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                sample = json.loads(line)
                break

    if sample is None:
        st.warning("Selected file is empty.")
    else:
        st.json(sample)
else:
    st.info("No batch files found yet. Run `python run_ingest.py` to start downloading.")
