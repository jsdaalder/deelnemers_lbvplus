"""Copy final outputs (master CSV + chart overview) into repo-level final_results with date tags."""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd

PIPE_ROOT = Path(__file__).resolve().parents[1]  # pipelines/uitgekochte
REPO_ROOT = Path(__file__).resolve().parents[3]  # repo root
PROCESSED_DIR = PIPE_ROOT / "data" / "processed"
DEFAULT_MASTER = PROCESSED_DIR / "master_permits.csv"
DEFAULT_CHARTS_DIR = PROCESSED_DIR / "charts"
DEFAULT_FINAL_ROOT = REPO_ROOT / "final_results"
EXPORT_STEM = "farms_permits_minfin"
DEFAULT_KEEP_COLS = [
    # Identifiers and company
    "farm_id",
    "rel_anoniem",
    "UBN",
    "cluster_id",
    "FICTIEF_BEDRIJFSNUMMER",
    "combined_company_names",
    "combined_kvk_numbers",
    "combined_address",
    # Geo/cluster coords
    "cluster_x_rd",
    "cluster_y_rd",
    # Animal/house info
    "Huisvesting",
    "rav_code",
    "stal",
    "gem_aantal_dieren",
    "gem_jaar",
    # LBV+ summary
    "lbv_plus_tot_dep",
    "lbv_plus_rank",
    # Publication/meta
    "Datum_latest",
    "Instantie_latest",
    "stage_latest_llm",
    "URL_BEKENDMAKING",
    "source",
    "link_method",
]


def copy_with_date(src: Path, dest_dir: Path, stem: str, date_tag: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{stem}_{date_tag}{src.suffix}"
    shutil.copy2(src, dest)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Export final CSV and chart overview to final_results/<date>.")
    parser.add_argument("--master", type=Path, default=DEFAULT_MASTER, help="Path to master_permits.csv.")
    parser.add_argument("--charts-dir", type=Path, default=DEFAULT_CHARTS_DIR, help="Directory containing charts.")
    parser.add_argument(
        "--final-root",
        type=Path,
        default=DEFAULT_FINAL_ROOT,
        help="Root folder for dated final_results.",
    )
    parser.add_argument(
        "--date-tag",
        type=str,
        default=datetime.now().strftime("%Y_%m_%d"),
        help="Date tag for exported filenames (default: today, YYYY_MM_DD).",
    )
    parser.add_argument(
        "--keep-cols",
        type=str,
        nargs="+",
        default=DEFAULT_KEEP_COLS,
        help="Columns to keep in the exported master CSV. Nonexistent columns are ignored.",
    )
    args = parser.parse_args()

    master_path = args.master.expanduser().resolve()
    charts_dir = args.charts_dir.expanduser().resolve()
    final_root = args.final_root.expanduser().resolve()
    date_tag = args.date_tag

    if not master_path.exists():
        raise FileNotFoundError(f"master CSV not found: {master_path}")
    if not charts_dir.exists():
        raise FileNotFoundError(f"charts directory not found: {charts_dir}")

    # Pick the overview files if present; fall back to chart_all.png.
    overview_pdf = charts_dir / "charts_overview.pdf"
    overview_png = charts_dir / "chart_all.png"
    if not overview_pdf.exists() and not overview_png.exists():
        raise FileNotFoundError(f"No overview chart found in {charts_dir} (expected charts_overview.pdf or chart_all.png)")

    dated_dir = final_root / date_tag
    copied = []

    # Write a slimmed version of the master CSV with selected columns.
    df = pd.read_csv(master_path)
    # Build combined address column: gather all available addresses (permit + KVK) and de-duplicate.
    def normalize_house_number(val: object) -> str:
        if pd.isna(val):
            return ""
        s = str(val).strip()
        # Convert floats like '9.0' to '9' when integral
        try:
            if "." in s:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
        except Exception:
            pass
        return s

    def normalize_postcode(val: object) -> str:
        if pd.isna(val):
            return ""
        s = str(val).strip().upper().replace(" ", "")
        return s

    def format_addr(straat, nr, toevoeg, pc, plaats):
        parts = []
        straat = "" if pd.isna(straat) else str(straat).strip()
        nr = normalize_house_number(nr)
        toevoeg = "" if pd.isna(toevoeg) else str(toevoeg).strip()
        pc = normalize_postcode(pc)
        plaats = "" if pd.isna(plaats) else str(plaats).strip()
        if straat:
            parts.append(straat)
        if nr or toevoeg:
            parts.append(" ".join([p for p in [nr, toevoeg] if p]))
        if pc:
            parts.append(pc)
        if plaats:
            parts.append(plaats)
        return ", ".join(p for p in parts if p)

    def dedup_preserve(seq):
        seen = set()
        out = []
        for item in seq:
            key = item.lower().strip()
            if key and key not in seen:
                seen.add(key)
                out.append(item)
        return out

    def collect_addresses(row) -> str:
        addrs = []
        addrs.append(
            format_addr(
                row.get("B_STRAATNAAM", ""),
                row.get("B_HUIS_NR", ""),
                row.get("B_HUIS_NR_TOEV", ""),
                row.get("B_POSTCODE", ""),
                row.get("B_PLAATS", ""),
            )
        )
        addrs.append(
            format_addr(
                row.get("kvk_api_straat", ""),
                row.get("kvk_api_huisnummer", ""),
                "",
                row.get("kvk_api_postcode", ""),
                row.get("kvk_api_plaats", ""),
            )
        )
        addrs = [a for a in addrs if a.strip()]
        addrs = dedup_preserve(addrs)
        return " | ".join(addrs)

    df["combined_address"] = df.apply(collect_addresses, axis=1)

    # Combined company names across all relevant columns.
    def collect_names(row) -> str:
        names = [
            row.get("COMPANY_NAME", ""),
            row.get("ontvanger", ""),
            row.get("company_name", ""),
            row.get("fos_naam", ""),
            row.get("kvk_api_name", ""),
        ]
        cleaned = []
        for n in names:
            if pd.isna(n):
                continue
            s = str(n).strip()
            if s:
                cleaned.append(s)
        names = dedup_preserve(cleaned)
        return " | ".join(names)

    df["combined_company_names"] = df.apply(collect_names, axis=1)

    # Combined KVK numbers across known sources (permit/minfin/fosfaat/KVK API).
    def collect_kvk(row) -> str:
        nums = [
            row.get("kvk_nummer_minfin", ""),
            row.get("fos_kvk", ""),
            row.get("kvk_api_number", ""),
        ]
        cleaned = []
        for n in nums:
            if pd.isna(n):
                continue
            s = str(n).strip()
            if s:
                cleaned.append(s)
        cleaned = dedup_preserve(cleaned)
        return " | ".join(cleaned)

    df["combined_kvk_numbers"] = df.apply(collect_kvk, axis=1)

    keep_cols = [c for c in args.keep_cols if c in df.columns]
    # Drop duplicate columns in keep order
    seen = set()
    dedup_keep_cols = []
    for c in keep_cols:
        if c not in seen:
            seen.add(c)
            dedup_keep_cols.append(c)
    keep_cols = dedup_keep_cols
    if keep_cols:
        df = df[keep_cols]
    slim_path = dated_dir / f"{EXPORT_STEM}_{date_tag}.csv"
    dated_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(slim_path, index=False)
    copied.append(slim_path)

    if overview_pdf.exists():
        copied.append(copy_with_date(overview_pdf, dated_dir, "charts_overview", date_tag))
    if overview_png.exists():
        copied.append(copy_with_date(overview_png, dated_dir, "chart_all", date_tag))

    print(f"[info] Exported {len(copied)} file(s) to {dated_dir}:")
    for path in copied:
        print(f" - {path}")


if __name__ == "__main__":
    main()
