# Synthesis workflow

This guide describes how to generate synthetic SQL queries with the reorganised codebase.

## 1. Choose a synthesis strategy

The generator supports multiple strategies:

- `schema_guided` – deterministic templates driven by handcrafted specifications.
- `llm_based` – prompts that query an LLM for query generation.
- `schema_guided_llm_refinement` – hybrid approach that refines template outputs with LLM feedback.

Each strategy writes artefacts to `data/processed/synthetic_queries/<strategy>/`.

## 2. Run the helper script

A convenience shell script is available in [`tools/scripts/analyze_synthetic_SQL.sh`](../../tools/scripts/analyze_synthetic_SQL.sh). It accepts a range of flags for specifying the target database, number of queries, and output options:

```bash
bash tools/scripts/analyze_synthetic_SQL.sh \
  --synthesis_method schema_guided \
  --db_name academic \
  --num_queries 50 \
  --write_to_csv true
```

The script validates that `data/processed/synthetic_queries` exists before proceeding.

## 3. Generate specifications programmatically

For custom workflows, call the generator module directly:

```python
from sql_benchmark.query_generation import query_generator_from_specifications

query_generator_from_specifications.main([
    "--schema-path", "data/raw/tables.json",
    "--output-dir", "data/processed/synthetic_queries/schema_guided",
])
```

The module manages checkpoints under `data/processed/synthetic_queries` to allow resuming long-running jobs.

## 4. Track progress and results

Generated CSV files and logs accumulate under the processed data folder. Summaries can be produced with the analysis helpers referenced in [`docs/workflows/evaluation.md`](evaluation.md).

If you introduce a new synthesis method, add a dedicated subfolder under `data/processed/synthetic_queries` and document the approach here.
