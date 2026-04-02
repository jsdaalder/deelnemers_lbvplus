#!/usr/bin/env python3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


def main() -> None:
    infile = Path("data/06_deelnemers_lbv_lbvplus.csv")
    outfile = Path("data/06b_province_stage_overview.csv")
    report_date = datetime.now().date()
    cutoff = report_date - timedelta(days=42)

    raw = pd.read_csv(infile)
    stage = raw["stage_latest_manual"].fillna("")
    stage = stage.replace("", pd.NA)
    stage = stage.combine_first(raw["stage_latest_llm"])
    raw["stage_effective"] = stage

    farm_latest = raw[
        ["farm_id", "Instantie_latest", "stage_effective", "Datum_latest"]
    ].drop_duplicates(subset=["farm_id"]).copy()
    farm_latest["Datum_latest"] = pd.to_datetime(
        farm_latest["Datum_latest"], errors="coerce"
    )

    rows = []
    for prov in sorted(farm_latest["Instantie_latest"].unique()):
        subset = farm_latest[farm_latest["Instantie_latest"] == prov]
        rows.append(
            {
                "province": prov,
                "total_farms": subset["farm_id"].nunique(),
                "receipt_of_application": subset[
                    subset["stage_effective"] == "receipt_of_application"
                ]["farm_id"].nunique(),
                "draft_decision": subset[
                    subset["stage_effective"] == "draft_decision"
                ]["farm_id"].nunique(),
                "definitive_decision": subset[
                    subset["stage_effective"] == "definitive_decision"
                ]["farm_id"].nunique(),
                f"irrevocable_on_{report_date.strftime('%Y_%m_%d')}": subset[
                    (subset["stage_effective"] == "definitive_decision")
                    & (subset["Datum_latest"] <= pd.Timestamp(cutoff))
                ]["farm_id"].nunique(),
            }
        )

    df_out = pd.DataFrame(rows)
    df_out.to_csv(outfile, index=False)
    print(f"[info] Wrote {outfile} ({len(df_out)} rows).")


if __name__ == "__main__":
    main()
