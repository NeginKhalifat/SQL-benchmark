# SynthSQL Benchmark Toolkit

## Objectives

Evaluating large language models (LLMs) on text-to-SQL tasks is challenging because strong performance may result from genuine reasoning or simple pattern memorization. **SynthSQL** provides a fully configurable framework for generating synthetic text-to-SQL benchmarks with explicit **difficulty control**, enabling rigorous evaluation of model generalization beyond familiar training distributions.

Our experiments show that state-of-the-art methods (DIN-SQL, DAIL-SQL) underperform a **Chain-of-Thought (CoT)** prompting baseline, especially on harder queries. This gap highlights both the need for stronger NLQ generation and the difficulty of generalizing to novel queries. A human evaluation study is planned to validate dataset quality and difficulty calibration.

---

## Key Features

* **Tunable SQL Difficulty:** Control join depth, aggregation, grouping, and set operations through configuration files.
* **Modular Pipeline:** Swap in alternative LLMs, NLQ generators, or evaluation protocols without touching core scripts.
* **Reproducibility:** Deterministic specification hashes and cached outputs enable exact reruns.
* **Built-in Analytics:** Scripts report clause coverage, difficulty buckets, and structural statistics.
* **Seamless Evaluation:** Integration with **Test-Suite-SQL** for execution-based metrics on Spider and synthetic datasets.

---

## Repository Structure

| Directory / File               | Description                                                       |
| ------------------------------ | ----------------------------------------------------------------- |
| `query_generation/`            | SQL specification and query synthesis framework.                  |
| `analysis/`                    | Scripts for statistics, coverage, and difficulty analysis.        |
| `sql2text/`                    | Back-translation utilities for generating NLQs.                   |
| `parser_sql/`                  | SQL parsers and Spider-compatible grammars.                       |
| `scripts/`                     | Pipeline automation helpers.                                      |
| `data/`                        | Spider datasets, metadata, and generated synthetic corpora.       |
| `outputs/`                     | Experiment artifacts: evaluation summaries, predictions, exports. |
| `test-suite-sql-eval-master/`  | Official semantic evaluation toolkit.                             |
| `text2sql-llms/`               | Prompt templates and evaluation scripts for baselines.            |
| `requirements.txt`, `setup.py` | Dependency list and install metadata.                             |

---

## Environment Setup

1. **Requirements**

   * Python 3.9+
   * `sqlite3` (optional for inspecting databases)
   * LLM provider API keys (OpenAI, Google, etc.)
   * Add Spider database into `test-suite-sql-eval-master/database`

2. **Install Dependencies**

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

---

## Quickstart Workflow

The SynthSQL pipeline has **five modular stages** — you can run them end-to-end or individually.

### 1. Generate Specifications

```bash
python query_generation/specification_generator_using_ht.py \
  --db_name concert_singer \
  --num_samples 500 \
  --output_dir query_generation/output/specs
```

Specifications encode structural patterns (e.g., join depth, predicates, aggregations) to ensure broad coverage across difficulty levels.

---

### 2. Synthesize SQL Queries

```bash
bash scripts/synthesis_SQL.sh \
  --db_name concert_singer \
  --num 200 \
  --write_to_csv \
  --synthesis_method schema_guided_llm_refinement \
  --config_name config_file2.json \
  --refinement_k 5
```

Generates queries with metadata (difficulty, clause annotations) stored under `data/synthetic-queries/<method>/`.

---

### 3. Analyze Synthetic SQL

```bash
bash scripts/analyze_synthetic_SQL.sh \
  --synthesis_method schema_guided \
  --num_queries 200 \
  --input_dir data/synthetic-queries/schema_guided
```

Reports coverage statistics, hardness buckets (`easy`, `medium`, `hard`, `extra`), and table/nesting counts.

---

### 4. Convert SQL to Natural Language

```bash
cd sql2text/back_translation
python back_translation
```

Prompts an LLM to generate NLQs, verifies via round-trip SQL reconstruction, and writes aligned SQL–NLQ pairs for evaluation.

---

### 5. Evaluate with Test-Suite-SQL

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

Use `--etype exec` for execution accuracy only.

---

## Data Assets

* **Spider schema & splits:** `tables.json`, `train_spider.json`, `dev.json`, `dev22.json`
* **Synthetic corpora:** `data/synthetic-queries/<method>` (CSV with SQL, spec hash, difficulty, etc.)
* **LLM refinements:** Filtered JSONs for downstream experiments
* **Evaluation outputs:** Stored in `outputs/experiments/` for reproducibility

---

## Evaluation and Benchmarking

You can benchmark any model (e.g., DIN-SQL, DAIL-SQL, GPT-4) on SynthSQL using **Test-Suite-SQL** for execution and semantic accuracy. For reproducibility, keep specification hashes and outputs version-controlled.

---

## Extending the Benchmark

* Add new databases and adjust configuration weights to explore different SQL phenomena.
* Replace the NLQ module with alternative back-translation strategies.
* Incorporate new evaluation metrics (e.g., structure-aware similarity, fine-grained error tags).
