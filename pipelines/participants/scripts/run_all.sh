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

maybe_step1() {
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
    refresh_flag=()
    if [[ -n "$REFRESH_ALL" ]]; then
      refresh_flag=(--refresh-all)
    fi
info "Step 01: parse overheid pages (local)"
"$PYTHON" scripts/01_parse_overheid_pages.py \
  --mode local \
  --files "${FILE_ARR[@]}" \
  --out data/01_overheid_results.csv \
  "${refresh_flag[@]}"
  elif [[ "$MODE" == "api" ]]; then
    if [[ -z "$API_QUERY" ]]; then
      die "MODE=api requires API_QUERY='...'"
    fi
    refresh_flag=()
    if [[ -n "$REFRESH_ALL" ]]; then
      refresh_flag=(--refresh-all)
    fi
    info "Step 01: parse overheid pages (API)"
    "$PYTHON" scripts/01_parse_overheid_pages.py \
      --mode api \
      --api-query "$API_QUERY" \
      --api-max-records "$API_MAX" \
      --api-timeout "$API_TIMEOUT" \
      --out data/01_overheid_results.csv \
      "${refresh_flag[@]}"
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
"$PYTHON" - <<'PY'
import pandas as pd
from datetime import datetime, timedelta

infile = "data/06_deelnemers_lbv_lbvplus.csv"
report_date = datetime.now().date()
cutoff = report_date - timedelta(days=42)

raw = pd.read_csv(infile)
stage = raw["stage_latest_manual"].fillna("")
stage = stage.replace("", pd.NA)
stage = stage.combine_first(raw["stage_latest_llm"])
raw["stage_effective"] = stage

farm_latest = raw[["farm_id", "Instantie_latest", "stage_effective", "Datum_latest"]].drop_duplicates(subset=["farm_id"]).copy()
farm_latest["Datum_latest"] = pd.to_datetime(farm_latest["Datum_latest"], errors="coerce")

rows = []
for prov in sorted(farm_latest["Instantie_latest"].unique()):
    subset = farm_latest[farm_latest["Instantie_latest"] == prov]
    total = subset["farm_id"].nunique()
    receipt = subset[subset["stage_effective"] == "receipt_of_application"]["farm_id"].nunique()
    draft = subset[subset["stage_effective"] == "draft_decision"]["farm_id"].nunique()
    definitive = subset[subset["stage_effective"] == "definitive_decision"]["farm_id"].nunique()
    irrevocable = subset[
        (subset["stage_effective"] == "definitive_decision")
        & (subset["Datum_latest"] <= pd.Timestamp(cutoff))
    ]["farm_id"].nunique()
    rows.append(
        {
            "province": prov,
            "total_farms": total,
            "receipt_of_application": receipt,
            "draft_decision": draft,
            "definitive_decision": definitive,
            f"irrevocable_on_{report_date.strftime('%Y_%m_%d')}": irrevocable,
        }
    )

info "Step 07: sync participants to matching pipelines"
DEST_DIR_FTM="$REPO_ROOT/pipelines/matching_ftm/data/raw"
mkdir -p "$DEST_DIR_FTM"
cp data/06_deelnemers_lbv_lbvplus.csv "$DEST_DIR_FTM/06_deelnemers_lbv_lbvplus.csv"
info "Synced to $DEST_DIR_FTM/06_deelnemers_lbv_lbvplus.csv for downstream matching pipeline"

info "Pipeline complete"
