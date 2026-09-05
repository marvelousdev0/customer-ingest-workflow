# PR Intelligence

This repository includes `.github/workflows/pr-intelligence.yml` to:

- Score PR **complexity** and surface a risk hint
- Auto-insert a **Mermaid impact flowchart** into the PR description
- Post an **AI review comment** (free-first model rotation, heuristic fallback)
- Run dependency vulnerability checks (GitHub dependency review + OSV)

PR authors start from `.github/PULL_REQUEST_TEMPLATE.md`. The workflow keeps the human-written sections and refreshes only the block between `<!-- pr-intel:start -->` and `<!-- pr-intel:end -->`.

## What the workflow does

1. Calculates a **complexity score** (0–100) from changed files, LOC delta, affected layers, and build-file changes.
2. Writes/updates the PR description block between:
   - `<!-- pr-intel:start -->`
   - `<!-- pr-intel:end -->`
3. Generates a **Mermaid flowchart** of the ingest layers (`consumer` → `service` → `redis` / `mongo` / `featureflags` → `producer`, plus `config` / `infra`). Changed nodes are highlighted in orange.
4. Posts an **AI review comment** (updated in-place on subsequent pushes) between:
   - `<!-- pr-intel-ai:start -->`
   - `<!-- pr-intel-ai:end -->`
5. Runs dependency scans with:
   - `actions/dependency-review-action` (fails on HIGH severity)
   - `google/osv-scanner-action`

## Free-first model rotation

By default, the AI step tries models in this order:

1. `gemini:gemini-2.0-flash-lite`
2. `openrouter:meta-llama/llama-3.1-8b-instruct:free`
3. `openrouter:google/gemma-2-9b-it:free`
4. `groq:llama-3.1-8b-instant`

Paid models are hard-blocked unless `ALLOW_PAID_MODELS` is set to `true` on the `ai-review` job `env`.

When enabled, additional paid candidates are appended:

1. `anthropic:claude-3-5-haiku-latest`
2. `openai:gpt-4o-mini`

Override with the `AI_REVIEW_MODEL_ORDER` job env value (comma-separated):

```text
openrouter:google/gemma-2-9b-it:free,groq:llama-3.1-8b-instant
```

## Secrets / variables

Provider keys are optional. Create them under **Settings → Secrets and variables → Actions**, then wire each one on the `ai-review` job `env` in `.github/workflows/pr-intelligence.yml` (replace `''` with `${{ secrets.NAME }}`). The Actions editor warns on `secrets.NAME` until that secret exists, so the workflow ships with empty defaults and uses heuristic review until you wire them.

| Name | Type | Purpose |
| ------ | ------ | --------- |
| `GEMINI_API_KEY` | Secret | Google Gemini (free tier available) |
| `OPENROUTER_API_KEY` | Secret | OpenRouter free models |
| `GROQ_API_KEY` | Secret | Groq free tier |
| `ANTHROPIC_API_KEY` | Secret | Claude (paid; requires `ALLOW_PAID_MODELS=true`) |
| `OPENAI_API_KEY` | Secret | OpenAI (paid; requires `ALLOW_PAID_MODELS=true`) |

Other knobs on the same job `env`:

| Name | Default | Purpose |
| ------ | --------- | --------- |
| `ALLOW_PAID_MODELS` | `false` | Set to `true` to allow paid providers |
| `AI_REVIEW_MODEL_ORDER` | _(empty → built-in free-first list)_ | Optional override of the model rotation order |

If no provider key is set, the workflow falls back to a **deterministic heuristic review** and still posts a comment on the PR.

## Complexity scoring

| Metric | Points |
| -------- | -------- |
| Each changed file | +2 |
| Each 10 LOC changed | +1 |
| Each affected layer | +8 |
| Build files changed | +20 |
| Hot-path (`service` / `consumer` / `producer`) changed | +12 |
| Secrets / Vault / k8s changed | +15 |
| More than 20 files changed | +15 |
| More than 500 LOC changed | +10 |

| Score | Risk |
| ------- | ------ |
| 0–39 | Low |
| 40–69 | Medium |
| 70–100 | High |

## Notes

- `dependency-review-action` fails on HIGH severity and above.
- The workflow updates a single AI review comment per PR using hidden markers to avoid comment spam.
- For private repositories, AI provider free-tier quotas still apply per provider account.
