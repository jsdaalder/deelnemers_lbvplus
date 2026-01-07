LLM improvement testing

- Purpose: quick experiments around stage classification accuracy and address handling for LBV/LBV+ publications.
- Key artifacts: `stage_run_results.json` (kept for transparency), supporting CSVs (`manual_stage_truth.csv`, `mismatches_latest.csv`, `results_latest.csv`) that stay git-ignored to avoid pushing bulky/PII-like data.
- Script: `stage_classifier_test.py` contains the test harness used in this folder.
- Validation notes (current):
  - Manual set contains 577 labeled rows, representing 512 unique notices (URLs) and 348 unique doc_id_latest.
  - All draft/definitive notices in the current corpus have now been manually reviewed.
  - Manual review found 8 corrections on previously LLM-labeled draft/definitive notices (no URLs listed here).
  - Earlier validation run (287 unique notices) scored 284/287 (98.96%); this is kept as historical context.
  - Prompt rules were tightened around "ontwerpbesluit" (current vs past/future context) and "voornemen/ontwerpbeschikking".
  - Deterministic override `has_current_ontwerpbesluit` was added to force draft when the current-ter-inzage pattern is detected (unless receipt_of_application).
  - Extra sanity test on 20 additional receipt notices mentioning ontwerpbesluit yielded 0 regressions.
- Status: exploratory; nothing automatically feeds the main pipeline. Update this README with findings if you re-run tests or change prompt/logic.
