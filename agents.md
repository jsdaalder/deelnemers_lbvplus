# LLM agents

This repo currently defines two OpenAI-powered “agents” in `scripts/04_ai_classify_lbv_and_addresses.py`. Both consume the combined HTML/PDF text from each publication and return structured JSON that feeds the downstream CSVs.

## 1. LBV classification agent

- **Location:** `SYSTEM_PROMPT_FULL` / `USER_TEMPLATE_FULL` inside `scripts/04_ai_classify_lbv_and_addresses.py`.
- **Purpose:** Determine whether the publication concerns the Landelijke beëindigingsregeling (LBV/LBV+), whether a Natura 2000 permit is being withdrawn (scope full/partial/unknown), and which procedural stage the notice represents (receipt, draft, definitive decision, intent notice, other).
- **Address extraction:** Also extracts the primary farm address (street, number, suffix, postcode, place) and returns a confidence score for both the classification and the address.
- **Model:** Defaults to `gpt-4.1-mini`, configurable via `DEFAULT_MODEL`. Responses must be valid JSON with the schema embedded in `USER_TEMPLATE_FULL`.
- **Usage tips:**
  - Concatenate `TEXT_HTML` and `TEXT_PDF` before prompting so the agent sees all available context.
  - Lower the confidence when signals are weak or ambiguous; downstream code respects confidence thresholds.
  - Ensure `.env` supplies `OPENAI_API_KEY` before running the script.

## 2. Address-only fallback agent

- **Location:** `SYSTEM_PROMPT_ADDR_ONLY` / `USER_TEMPLATE_ADDR_ONLY` in the same script.
- **Purpose:** When only the address is needed (or when the main classification agent fails), this prompt focuses solely on extracting the main location details.
- **Behavior:** Returns the same address schema as above but omits LBV fields.
- **When to use:** Trigger from custom tooling if the LBV agent times out or if you want to re-parse addresses without reclassifying LBV status.

## Prompt maintenance

- Keep the system prompts in Dutch, matching the source material and encouraging conservative classifications.
- When changing output schemas, update both the prompt templates and the CSV-writing logic so downstream code remains aligned.
- Consider logging the prompt/response pairs locally (never in git) while tuning instructions to diagnose edge cases.
