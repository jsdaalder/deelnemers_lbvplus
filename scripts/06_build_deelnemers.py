import argparse
import pandas as pd
from collections import defaultdict
from pathlib import Path


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        # Path compression
        if self.parent.setdefault(x, x) != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
# Accepts the step 05 output directly; legacy 06_vergunningen files are archived.
DEFAULT_INPUT = DATA_DIR / "05_lbv_enriched_addresses.csv"
DEFAULT_OUTPUT = DATA_DIR / "06_deelnemers_lbv_lbvplus.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Groepeer vergunning-publicaties op boerderijniveau."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Input CSV met verrijkte vergunningen (default: data/05_lbv_enriched_addresses.csv).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV met deelnemers (default: data/06_deelnemers_lbv_lbvplus.csv).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optioneel maximum aantal rijen voor snelle checks.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ---------- 1. Load input ----------
    infile = Path(args.input).expanduser().resolve()
    outfile = Path(args.output).expanduser().resolve()

    # Read as strings; we'll parse dates separately
    df = pd.read_csv(infile, dtype=str)
    if args.max_rows is not None:
        df = df.head(args.max_rows)

    # If there's an existing farm_id column, ignore/replace it
    if "farm_id" in df.columns:
        df = df.drop(columns=["farm_id"])

    # Parse Datum (supports dd/mm/yyyy and dd-mm-yyyy)
    df["Datum"] = pd.to_datetime(df["Datum"], dayfirst=True, errors="coerce")

    # Basic cleaning: require doc_id, AddressKey, Datum
    df = df[df["doc_id"].notna() & (df["doc_id"] != "")]
    df = df[df["AddressKey"].notna() & (df["AddressKey"] != "")]
    df = df[df["Datum"].notna()]

    # ---------- 2. Build mapping: AddressKey -> doc_ids ----------
    addr_to_docs = defaultdict(set)

    for _, row in df[["doc_id", "AddressKey"]].drop_duplicates().iterrows():
        addr_to_docs[row["AddressKey"]].add(row["doc_id"])

    # ---------- 3. Union-Find over doc_ids ----------
    uf = UnionFind()

    # Union all doc_ids that share an AddressKey
    for doc_ids in addr_to_docs.values():
        doc_ids = list(doc_ids)
        if len(doc_ids) > 1:
            first = doc_ids[0]
            for other in doc_ids[1:]:
                uf.union(first, other)

    # Ensure every doc_id appears in the DSU structure
    for doc_id in df["doc_id"].unique():
        uf.find(doc_id)

    # ---------- 4. Map doc_ids to farm_ids ----------
    roots = sorted({uf.find(d) for d in df["doc_id"].unique()})
    root_to_farm_id = {
        root: f"FARM{str(i + 1).zfill(4)}" for i, root in enumerate(roots)
    }

    df["farm_root"] = df["doc_id"].apply(uf.find)
    df["farm_id"] = df["farm_root"].map(root_to_farm_id)

    # ---------- 5. Determine latest record per farm ----------
    # For each farm_id, take the row with the maximum Datum
    idx_latest_per_farm = df.groupby("farm_id")["Datum"].idxmax()
    latest = df.loc[idx_latest_per_farm].copy()

    latest_status = latest.set_index("farm_id")[[
        "doc_id",
        "Titel",
        "Datum",
        "Instantie",
        "STAGE",
        "URL_BEKENDMAKING",
        "URL_PDF",
    ]]

    # ---------- 6. Build output: one row per (farm_id, AddressKey) ----------
    # Unique farm/address combinations
    addresses = (
        df[[
            "farm_id",
            "AddressKey",
            "B_STRAATNAAM",
            "B_HUIS_NR",
            "B_HUIS_NR_TOEV",
            "B_POSTCODE",
            "B_PLAATS",
        ]]
        .drop_duplicates()
        .sort_values(["farm_id", "AddressKey"])
    )

    # Collect provenance: all doc_ids and address keys per farm
    doc_ids_all = (
        df.groupby("farm_id")["doc_id"]
        .unique()
        .apply(lambda vals: ",".join(sorted(vals)))
    )
    addresskeys_all = (
        df.groupby("farm_id")["AddressKey"]
        .unique()
        .apply(lambda vals: ",".join(sorted(vals)))
    )

    out = addresses.merge(
        latest_status,
        left_on="farm_id",
        right_index=True,
        how="left",
    )

    # Rename for clarity
    out = out.rename(columns={
        "doc_id": "doc_id_latest",
        "Titel": "Titel_latest",
        "Datum": "Datum_latest",
        "Instantie": "Instantie_latest",
        "STAGE": "stage_latest_llm",
    })

    # Provide an empty manual stage column next to the LLM stage
    out["stage_latest_manual"] = ""
    # ensure ordering keeps manual column next to llm column
    cols = list(out.columns)
    if "stage_latest_llm" in cols and "stage_latest_manual" in cols:
        cols.remove("stage_latest_manual")
        idx = cols.index("stage_latest_llm") + 1
        cols.insert(idx, "stage_latest_manual")
        out = out[cols]

    out["doc_ids_all"] = out["farm_id"].map(doc_ids_all)
    out["AddressKeyAll"] = out["farm_id"].map(addresskeys_all)

    # Optional: sort by farm_id then street for readability
    out = out.sort_values(["farm_id", "B_PLAATS", "B_STRAATNAAM", "B_HUIS_NR"])

    # ---------- 7. Save ----------
    out.to_csv(outfile, index=False)
    print(f"Wrote {outfile} with {len(out)} rows.")


if __name__ == "__main__":
    main()
