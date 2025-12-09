# Repository Guidelines

## Project Structure & Module Organization
- Target layout for analysis work:
  - `src/` for reusable pipelines, loaders, and feature engineering.
  - `notebooks/` for exploratory work; keep them lightweight and commit stripped outputs.
  - `data/` for raw/external inputs (git-ignored); add a `data/README.md` describing expected files.
  - `tests/` mirrors `src/` structure; one test module per source module.
  - `docs/` for methodology notes and decision logs.
- Keep configuration in version-controlled files (e.g., `pyproject.toml`, `.env.example`), never commit real secrets.

## Build, Test, and Development Commands
- Environment bootstrap (Python example): `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- Lint/format (if using Ruff/Black): `ruff check src tests` and `black src tests` to enforce consistency.
- Run unit tests: `pytest -q` from the repo root; add `-k <pattern>` to scope.
- Data checks (suggested): add lightweight smoke scripts under `scripts/` to validate input schema before runs.

## Coding Style & Naming Conventions
- Prefer Python 3.11+ where possible; use type hints on public functions and dataclasses for structured records.
- Indentation: 4 spaces; wrap lines at ~100 chars; avoid implicit globals in notebooks—lift logic into `src/`.
- Naming: functions `verb_noun` (e.g., `load_farm_data`), modules `snake_case.py`, classes `PascalCase`.
- Keep notebook cells short; move reusable code into `src/` and import it.

## Testing Guidelines
- Place tests in `tests/` with names like `test_<module>.py`; one behavioral concern per test.
- Use `pytest` fixtures for sample datasets; prefer deterministic seeds for any random sampling.
- Aim for coverage of parsing, aggregation logic, and edge cases around missing or malformed input rows.
- For notebooks, add a minimal regression test that executes key pipelines via `nbval` or a CLI wrapper.

## Commit & Pull Request Guidelines
- Commits: use concise, present-tense messages (`add loader for farm registry`; `fix aggregation null handling`).
- PRs: include scope, rationale, and before/after notes; link related issues or datasets; attach screenshots for plots or dashboards.
- Keep PRs small and reviewable; note any data assumptions, schema changes, or breaking interface updates.
- Add a short checklist: tests passing, lint clean, docs updated, and sample command used to verify changes.

## Security & Configuration Tips
- Store secrets in `.env` (git-ignored); provide `.env.example` with required keys or file paths.
- Treat raw data as sensitive; avoid committing personal information. Document anonymization steps in `docs/`.
- If using external APIs, pin dependencies and capture exact versions in lock files for reproducibility.
