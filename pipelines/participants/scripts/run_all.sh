#!/usr/bin/env bash
# Orchestrate the full LBV/LBV+ pipeline in one go.
# Requirements:
# - Python 3.11+ and dependencies installed (see README).
# - .env with OPENAI_API_KEY (needed for step 04).
# - Step 01 inputs:
#     MODE=local  FILES="prov1.html prov2.html"   (local HTML exports)
#     MODE=api    API_QUERY='c.product-area==officielepublicaties AND ...' [API_MAX=500]
#   Step 01 now runs by default (incremental via script logic). Set SKIP_STEP1=1 to keep existing results.

set -euo pipefail

# Pipeline root (participants) and repo root
PIPE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PIPE_ROOT/../.." && pwd)"
cd "$PIPE_ROOT"

PYTHON="${PYTHON:-python3}"
MODE="${MODE:-api}"
FILES="${FILES:-}"
API_QUERY="${API_QUERY:-}"
# Default API query if none provided (matches LBV/LBV+ provincial notices)
if [[ -z "$API_QUERY" ]]; then
  API_QUERY='c.product-area==officielepublicaties AND cql.textAndIndexes="lbv"'
fi
API_MAX="${API_MAX:-500}"
API_TIMEOUT="${API_TIMEOUT:-30}"
REFRESH_ALL="${REFRESH_ALL:-}"
SKIP_STEP1="${SKIP_STEP1:-}"
MAX_ROWS="${MAX_ROWS:-10000}"

info() { printf "\\n[info] %s\\n" "$*"; }
die() { printf "\\n[error] %s\\n" "$*" >&2; exit 1; }

# Basic checks
[ -x "$(command -v "$PYTHON")" ] || die "python3 not found"
[ -f "$REPO_ROOT/.env" ] || die "Missing .env at repo root (must define OPENAI_API_KEY for step 04)"
grep -q "OPENAI_API_KEY" "$REPO_ROOT/.env" || die "OPENAI_API_KEY not found in $REPO_ROOT/.env"
mkdir -p data

capture_counts() {
  local outfile="$1"
  "$PYTHON" - "$outfile" <<'PY'
import json
import sys
import pandas as pd
from pathlib import Path

outfile = sys.argv[1]
data_dir = Path("data")
overheid = data_dir / "01_overheid_results.csv"
participants = data_dir / "06_deelnemers_lbv_lbvplus.csv"

payload = {
    "unique_notices": 0,
    "unique_farms": 0,
    "latest_manual_labeled": 0,
    "latest_manual_unlabeled": 0,
}

if overheid.exists():
    df = pd.read_csv(overheid, dtype=str, keep_default_na=False)
    if "URL" in df.columns:
        payload["unique_notices"] = int(df["URL"].astype(str).str.strip().replace("", pd.NA).dropna().nunique())

if participants.exists():
    df = pd.read_csv(participants, dtype=str, keep_default_na=False)
    if "farm_id_new" in df.columns:
        farms = df.drop_duplicates(subset=["farm_id_new"]).copy()
        payload["unique_farms"] = int(farms["farm_id_new"].astype(str).str.strip().replace("", pd.NA).dropna().nunique())
        if "stage_latest_manual" in farms.columns:
            labeled = farms["stage_latest_manual"].astype(str).str.strip() != ""
            payload["latest_manual_labeled"] = int(labeled.sum())
            payload["latest_manual_unlabeled"] = int((~labeled).sum())

Path(outfile).write_text(json.dumps(payload), encoding="utf-8")
PY
}

print_counts_summary() {
  local before_json="$1"
  local after_json="$2"
  "$PYTHON" - "$before_json" "$after_json" <<'PY'
import json
import sys
from pathlib import Path

before = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
after = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

fields = [
    ("unique_notices", "unique notices"),
    ("unique_farms", "unique farms"),
    ("latest_manual_labeled", "latest notices manually labeled"),
    ("latest_manual_unlabeled", "latest notices missing manual label"),
]

print("")
print("[summary] Participants pipeline")
for key, label in fields:
    b = int(before.get(key, 0))
    a = int(after.get(key, 0))
    diff = a - b
    sign = f"+{diff}" if diff > 0 else str(diff)
    print(f"[summary] {label}: {a} (before={b}, change={sign})")
PY
}

TMP_DIR="data/runs"
mkdir -p "$TMP_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
BEFORE_JSON="$TMP_DIR/${STAMP}_before_run_summary.json"
AFTER_JSON="$TMP_DIR/${STAMP}_after_run_summary.json"

capture_counts "$BEFORE_JSON"

maybe_step1() {
  local meta_path="data/01_parse_meta.json"
  if [[ -n "$SKIP_STEP1" ]]; then
    info "Step 01: skipped because SKIP_STEP1 is set."
    return
  fi
  if [[ -z "$MODE" ]]; then
    die "Set MODE=local with FILES=\"...\" or MODE=api with API_QUERY='...'."
  fi
  if [[ "$MODE" == "local" ]]; then
    if [[ -z "$FILES" ]]; then
      die "MODE=local requires FILES=\"file1.html file2.html ...\""
    fi
    IFS=' ' read -r -a FILE_ARR <<< "$FILES"
    info "Step 01: parse overheid pages (local)"
    cmd=(
      "$PYTHON" scripts/01_parse_overheid_pages.py
      --mode local
      --files "${FILE_ARR[@]}"
      --out data/01_overheid_results.csv
      --meta-out "$meta_path"
    )
    if [[ -n "$REFRESH_ALL" ]]; then
      cmd+=(--refresh-all)
    fi
    "${cmd[@]}"
  elif [[ "$MODE" == "api" ]]; then
    if [[ -z "$API_QUERY" ]]; then
      die "MODE=api requires API_QUERY='...'"
    fi
    local continue_fetch="y"
    local start_record=1
    while [[ "$continue_fetch" == "y" ]]; do
      info "Step 01: parse overheid pages (API)"
      cmd=(
        "$PYTHON" scripts/01_parse_overheid_pages.py
        --mode api
        --api-query "$API_QUERY"
        --api-start-record "$start_record"
        --api-max-records "$API_MAX"
        --api-timeout "$API_TIMEOUT"
        --out data/01_overheid_results.csv
        --meta-out "$meta_path"
      )
      if [[ -n "$REFRESH_ALL" && "$start_record" == "1" ]]; then
        cmd+=(--refresh-all)
      fi
      "${cmd[@]}"

      local prompt
      prompt="$("$PYTHON" - "$meta_path" <<'PY'
import json
import sys
from pathlib import Path

meta = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
fetched = int(meta.get("fetched_rows", 0))
available = int(meta.get("api_total_available", 0))
cap = int(meta.get("api_max_records", 0))
start = int(meta.get("api_start_record", 1))
more = bool(meta.get("more_available", False))
next_start = start + fetched
accepted_total = 0
out_path = Path(meta.get("out_path", ""))
if out_path.exists():
    import pandas as pd
    df = pd.read_csv(out_path, dtype=str, keep_default_na=False)
    accepted_total = len(df)
if more and cap > 0 and fetched >= cap:
    print(f"FETCH_MORE {fetched} {available} {cap} {next_start} {accepted_total}")
PY
)"
      if [[ "$prompt" == FETCH_MORE\ * ]]; then
        read -r _ fetched available cap next_start accepted_total <<< "$prompt"
        if [[ -t 0 ]]; then
          printf "[info] Step 01 has processed a page of %s raw API hits out of %s total raw API hits; accepted scraper rows so far: %s.\n" "$fetched" "$available" "$accepted_total"
          read -r -p "Fetch the next page of up to ${cap} raw API hits? [y/N] " answer
          case "${answer:-n}" in
            y|Y|yes|YES)
              start_record="$next_start"
              continue
              ;;
          esac
        else
          printf "[warn] Step 01 reached the page cap after %s raw API hits; total raw API hits available: %s; accepted scraper rows so far: %s. Re-run interactively to fetch more pages.\n" "$fetched" "$available" "$accepted_total"
        fi
      fi
      continue_fetch="n"
    done
  else
    die "Unknown MODE=$MODE (use 'local' or 'api')."
  fi
}

maybe_step1

info "Step 02: enrich with HTML/PDF context"
"$PYTHON" scripts/02_enrich_with_html_and_pdfs.py \
  --in data/01_overheid_results.csv \
  --out data/02_lbv_enriched.csv

info "Step 03: extract PDF text"
"$PYTHON" scripts/03_extract_pdf_text.py \
  --in data/02_lbv_enriched.csv \
  --out data/03_lbv_enriched_with_pdf.csv

info "Step 04: LLM classification (LBV/LBV+, stage, address)"
"$PYTHON" scripts/04_ai_classify_lbv_and_addresses.py \
  --in data/03_lbv_enriched_with_pdf.csv \
  --out-csv data/04_lbv_enriched_with_ai_summary.csv \
  --mode full \
  --only-unclassified \
  --no-prompt \
  --limit "$MAX_ROWS"
if [[ "$MAX_ROWS" != "0" ]]; then
  info "Step 04 ran with MAX_ROWS=$MAX_ROWS (only-unclassified). If you expected more rows, rerun with MAX_ROWS=0 or a higher value."
fi

info "Step 05: deterministic address cleanup"
"$PYTHON" scripts/05_enrich_addresses.py \
  --input data/04_lbv_enriched_with_ai_summary.csv \
  --output data/05_lbv_enriched_addresses.csv

info "Step 06: aggregate participants"
"$PYTHON" scripts/06_build_deelnemers.py \
  --input data/05_lbv_enriched_addresses.csv \
  --output data/06_deelnemers_lbv_lbvplus.csv

info "Step 06b: rebuild province-stage overview"
"$PYTHON" scripts/06b_build_province_stage_overview.py

info "Step 08: classify LBV vs LBV+"
"$PYTHON" scripts/08_classify_lbv_scheme.py

info "Step 07: sync participants to matching pipelines"
DEST_DIR_FTM="$REPO_ROOT/pipelines/matching_ftm/data/raw"
mkdir -p "$DEST_DIR_FTM"
cp data/06_deelnemers_lbv_lbvplus.csv "$DEST_DIR_FTM/06_deelnemers_lbv_lbvplus.csv"
info "Synced to $DEST_DIR_FTM/06_deelnemers_lbv_lbvplus.csv for downstream matching pipeline"

capture_counts "$AFTER_JSON"
print_counts_summary "$BEFORE_JSON" "$AFTER_JSON"

info "Pipeline complete"
