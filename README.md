# SynthSQL Benchmark Toolkit

## Objectives

Evaluating large language models (LLMs) on text-to-SQL tasks is challenging because strong performance can stem from genuine reasoning or simple pattern memorization. SynthSQL addresses this gap with a fully configurable framework for generating synthetic text-to-SQL benchmarks whose query difficulty is under explicit control. By calibrating SQL complexity from easy to hard, SynthSQL enables rigorous evaluation of model generalization beyond familiar training distributions. The toolkit ships with both a modular query generator and a natural-language conversion module that produces questions answerable by the generated SQL.

Our experiments on the SynthSQL benchmark show that state-of-the-art methods (e.g., DIN-SQL and DAIL-SQL) underperform the chain-of-thought (CoT) prompting approach we introduced—particularly as query complexity increases. We attribute this gap to two factors: limitations of our natural language question generator and the methods’ difficulty generalizing to SynthSQL’s more challenging, novel queries. To further validate the dataset’s quality and difficulty calibration, we plan a human evaluation study.

These findings underscore the need for benchmarks that span a broader range of complexities than existing public datasets, enabling more accurate assessments of LLMs’ true text-to-SQL capabilities.

## Table of Contents

1. [Key Features](#key-features)
2. [Repository Structure](#repository-structure)
3. [Environment Setup](#environment-setup)
4. [Quickstart Workflow](#quickstart-workflow)
   1. [Generate Specifications](#1-generate-specifications)
   2. [Synthesize SQL Queries](#2-synthesize-sql-queries)
   3. [Analyze Synthetic SQL](#3-analyze-synthetic-sql)
   4. [Convert SQL to Natural Language Questions](#4-convert-sql-to-natural-language-questions)
   5. [Evaluate with Test-Suite-SQL](#5-evaluate-with-test-suite-sql)
5. [Data Assets](#data-assets)
6. [Configuration Reference](#configuration-reference)
7. [Evaluation and Benchmarking](#evaluation-and-benchmarking)
8. [Troubleshooting](#troubleshooting)
9. [Extending the Benchmark](#extending-the-benchmark)
10. [Contributing](#contributing)
11. [License](#license)
12. [Citation](#citation)
13. [Contact](#contact)

## Key Features

- **Tunable SQL difficulty:** Control join depth, aggregation density, grouping, and set operations through specification weights and configuration files.
- **Pipeline modularity:** Swap in alternative LLMs, NLQ generators, or evaluation protocols without editing core scripts.
- **Reproducible experiments:** Deterministic specification hashes and cached query outputs enable exact reruns of past experiments.
- **Comprehensive analytics:** Built-in scripts track clause coverage, hardness buckets, and structural statistics for generated corpora.
- **Integration with Test-Suite-SQL:** Leverage official execution-based evaluation for Spider and synthetic splits with minimal glue code.

## Repository Structure

| Directory / File | Description |
|------------------|-------------|
| `query_generation/` | SQL synthesis framework that produces specifications and SQL queries across difficulty levels. |
| `analysis/` | Notebooks and scripts for aggregating statistics, hardness metrics, and coverage analysis. |
| `sql2text/` | Back-translation utilities that convert SQL queries into natural language questions (NLQs). |
| `parser_sql/` | SQL parsing utilities and Spider-compatible grammars for structural analysis. |
| `scripts/` | Automation helpers for synthesis, statistics, evaluation, and environment bootstrapping. |
| `data/` | Canonical Spider datasets, schema metadata, and generated synthetic corpora. |
| `outputs/` | Stores experiment artifacts such as evaluation summaries, predictions, and per-database SQL exports. |
| `test-suite-sql-eval-master/` | Third-party semantic evaluation toolkit for SQL correctness. |
| `text2sql-llms/` | Prompt templates, execution scripts, and evaluation utilities for running LLM baselines. |
| `spider-statistics/` | Clause-level statistics and exploratory analysis for Spider and SynthSQL queries. |
| `research_questions/` | Documentation of open research directions, ablation notes, and experimental design. |
| `requirements.txt`, `setup.py` | Python dependency list and editable-install metadata. |

## Environment Setup

1. **Prerequisites**
   - Python 3.9+ (tested on 3.10)
   - `sqlite3` command-line tools for local database inspection (optional)
   - Access tokens for any external LLM providers you plan to use (OpenAI, Google Generative AI, etc.)
   - Add spider databse in test-suite-sql-eval-master/database
2. **Create an isolated environment and install dependencies:**

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

The helper script `scripts/synthesis_SQL.sh` can bootstrap the environment automatically if one does not already exist. Manual setup is recommended the first time to verify credentials and optional dependencies.

## Quickstart Workflow

The SynthSQL pipeline consists of five modular stages. Each stage can be run independently, enabling custom datasets, partial reruns, or experimentation with alternative components.

### 1. Generate Specifications

Specifications describe the structural and semantic patterns that the generator should realize. They are produced with `query_generation/specification_generator_using_ht.py`, which hashes combinations of FROM, WHERE, GROUP BY, and HAVING choices to ensure broad coverage across difficulty settings.

```bash
python query_generation/specification_generator_using_ht.py \
  --db_name concert_singer \
  --num_samples 500 \
  --output_dir query_generation/output/specs
```

Key parameters:
- `--db_name`: Target database (defaults to iterating across all available databases if omitted).
- `--num_samples`: Number of specification templates to instantiate.
- `--output_dir`: Destination for the generated specification hash tables.

Specifications capture heuristics such as join depth, predicate composition, and aggregation density via dictionaries like `FROM_WEIGHTS`, `WHERE_WEIGHTS`, and `GROUP_BY_WEIGHTS` within the script.

### 2. Synthesize SQL Queries

Use the high-level wrapper `scripts/synthesis_SQL.sh` to instantiate a virtual environment, install dependencies, and call `query_generation/query_generator_from_specifications.py` with your desired configuration.

```bash
bash scripts/synthesis_SQL.sh \
  --db_name concert_singer \
  --num 200 \
  --write_to_csv \
  --synthesis_method schema_guided_llm_refinement \
  --config_name config_file2.json \
  --refinement_k 5
```

Important flags:
- `--db_name`: Target Spider database. Use an empty string to iterate over all validation databases.
- `--num`: Number of candidate specifications to realize.
- `--write_to_csv`: Persist generated SQL and metadata in `query_generation/output/` and `data/synthetic-queries/<method>/`.
- `--random_choice`: Toggle deterministic sampling from the specification space.
- `--synthesis_method`: Choose among `schema_guided`, `llm_based`, or `schema_guided_llm_refinement` pipelines.
- `--config_name`: Select configuration files that enable advanced constructs such as set operations.
- `--refinement_k`: Bound the number of LLM refinement attempts per specification.

You can also call the Python entrypoint directly for scripted experiments:

```bash
python query_generation/query_generator_from_specifications.py \
  --db_name concert_singer \
  --num 100 \
  --write_to_csv True \
  --synthesis_method schema_guided \
  --config_name config_file.json
```

Generated CSVs store the SQL text, specification hash, predicted difficulty, and clause-level annotations for downstream filtering.

### 3. Analyze Synthetic SQL

After synthesis, quantify coverage and difficulty using `analysis/synthetic_statistics.py`. The helper `scripts/analyze_synthetic_SQL.sh` routes output summaries to `outputs/experiments/SyntheticBenchmarkEvaluation/<method>/`.

```bash
bash scripts/analyze_synthetic_SQL.sh \
  --synthesis_method schema_guided \
  --num_queries 200 \
  --input_dir data/synthetic-queries/schema_guided
```

The analysis script parses each SQL query with the Test-Suite evaluator to tally hardness buckets (`easy`, `medium`, `hard`, `extra`), table counts, and nesting frequency per database. Successful parses are cached in `correct_sqls/` folders for reproducibility, and the resulting JSON/CSV summaries support downstream visualization.

### 4. Convert SQL to Natural Language Questions

To generate synthetic NLQs, leverage the back-translation utilities in `sql2text/back_translation`. The `llama3_synthetic_data.py` workflow iteratively prompts an LLM to produce question candidates, verifies them via round-trip SQL reconstruction, and saves aligned pairs.

```bash
cd sql2text/back_translation
python back_translation
```

The script writes CSV files containing the original SQL, the synthesized question, iteration counts, and equivalence flags, enabling BLEU/ROUGE scoring or manual audits.

### 5. Evaluate with Test-Suite-SQL

Finally, benchmark model or LLM outputs using the official Test-Suite evaluation housed in `test-suite-sql-eval-master/`.

1. Download the Spider test-suite databases into `test-suite-sql-eval-master/database/` if they are not already present.
2. Run semantic execution accuracy checks:

```bash
cd test-suite-sql-eval-master
python evaluation.py \
  --gold ../data/dev22.json \
  --pred ../outputs/synthetic_predictions.sql \
  --db database \
  --table ../data/tables22.json \
  --etype all \
  --plug_value
```

Use `--etype exec` for execution-only metrics or omit `--plug_value` when evaluating queries with explicit value predictions. The evaluator also supports classical datasets (`evaluate_classical.py`) and cached runs for large-scale experiments.

## Data Assets

- **Spider schema metadata**: `data/tables.json` and `data/tables22.json` expose schema, primary key, and foreign key definitions for the Spider benchmark.
- **Spider splits**: `data/train_spider.json`, `data/dev.json`, and `data/dev22.json` provide ground-truth SQL and NLQ pairs for transfer comparisons.
- **Synthetic SQL corpora**: `data/synthetic-queries/<method>` contains generated CSVs for each database (`*_res.csv`) along with optional `correct_sqls` folders for filtered outputs.
- **LLM refinements**: JSON files such as `combined_filtered_schema_guided_llm_refinement.json` capture curated query sets for downstream experiments.
- **Evaluation outputs**: `outputs/experiments/` stores aggregated statistics, CSV reports, and archived prediction files.
