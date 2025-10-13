# Getting started

This guide walks through the minimum steps required to prepare a development environment and generate a small batch of synthetic queries.

## 1. Clone and bootstrap

```bash
git clone <repository-url>
cd SQL-benchmark
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Installing in editable mode ensures the `sql_benchmark` namespace resolves to `src/sql_benchmark` without modifying `PYTHONPATH` manually.

## 2. Verify package imports

Open a Python shell and run the following sanity check:

```python
from sql_benchmark.parser_sql.parse_sql_one import Schema, get_schema
from sql_benchmark.query_generation.helper_funcs.helper_funcs import random_not_pk_cols
print("Loaded sql_benchmark package successfully!")
```

If the import fails, confirm that the virtual environment is activated and the repository root contains the `src/` directory.

## 3. Inspect the available data

Raw Spider tables and splits now live in [`data/raw`](../data/raw). Processed artefacts (for example, synthetic query buckets) are located in [`data/processed`](../data/processed). The [`docs/data-guide.md`](data-guide.md) reference provides additional context for each dataset.

## 4. Generate queries for a schema

The `query_generation` package exposes functions for creating specifications and SQL statements. The example below triggers the specification-driven generator:

```bash
python -m sql_benchmark.query_generation.query_generator_from_specifications \
  --schema-path data/raw/tables.json \
  --output-dir data/processed/synthetic_queries/schema_guided
```

The generator writes outputs to the structured `data/processed/synthetic_queries` directory. Consult [`docs/workflows/synthesis.md`](workflows/synthesis.md) for larger-scale orchestration.

## 5. Run evaluation helpers

Utility scripts in [`tools/evaluation`](../tools/evaluation) aggregate evaluation subsets, while [`tools/scripts/analyze_synthetic_SQL.sh`](../tools/scripts/analyze_synthetic_SQL.sh) coordinates an end-to-end synthetic benchmark analysis. Refer to [`docs/workflows/evaluation.md`](workflows/evaluation.md) for detailed guidance.

With the project bootstrapped, continue exploring the module map in [`docs/module-map.md`](module-map.md) to understand where new features belong.
