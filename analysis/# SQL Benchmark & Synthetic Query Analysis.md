# SQL Benchmark & Synthetic Query Analysis

## Overview

This repository provides tools and scripts for evaluating SQL benchmarks, generating synthetic queries, and analyzing the conversion between SQL and natural language queries (NLQ). It includes utilities for running local analyses, calculating statistics, and visualizing results.

## Project Structure

- `analysis/`: Contains scripts for statistical analysis and evaluation.
  - `analysis_res.py`: Utilities for running the test-suite-sql-eval analysis locally, including batch evaluation of LLM predictions.
  - `synthetic_statistics.py`: Calculates statistics on synthetic SQL queries, such as query hardness and table usage.
  - `sql2text_analysis.py`: Evaluates and visualizes the conversion of SQL queries to natural language, including success rates and query hardness analysis.
- `test-suite-sql-eval-master/`: External evaluation suite for SQL benchmarks.
- `outputs/`: Stores generated analysis results and visualizations.
- Other folders and files for query generation, configuration, and results.

## Usage

### 1. Running Analysis Scripts

#### Synthetic Statistics

Analyze synthetic SQL queries and compute statistics:

```bash
python analysis/synthetic_statistics.py --synthesis_method llm_based --output_file outputs/synthetic_stats.txt
```

#### SQL-to-Text Analysis

Evaluate and visualize SQL-to-NLQ conversion success rates:

```bash
python analysis/sql2text_analysis.py --synthesis_method llm_based
```

#### Batch Evaluation of LLMs

Run evaluation for multiple LLMs and databases:

```bash
python -m analysis.analysis_res --llms gpt-3.5-turbo gpt-4 --base-path test-suite-sql-eval-master --output-dir outputs/analysis
```

### 2. Configuration

- Update paths and parameters in scripts as needed for your environment.
- Place your gold and prediction files under `test-suite-sql-eval-master/evaluation_examples/<llm>/`.

## Analysis Scripts

### `analysis/analysis_res.py`

- Discovers databases with gold files and runs the evaluation script for each LLM and database.
- Stores logs in the specified output directory.
- Modular design for easy extension and testing.

### `analysis/synthetic_statistics.py`

- Concatenates CSV results, computes query hardness, table usage, and nested query statistics.
- Supports multiple synthesis methods (`schema_guided`, `llm_based`, etc.).
- Outputs summary statistics and averages per category.

### `analysis/sql2text_analysis.py`

- Adds query hardness labels to SQL-to-text conversion results.
- Computes success rates by iteration and query hardness.
- Generates visualizations for cumulative success rates and hardness analysis.

## Visualizations

Analysis scripts generate plots and summary files in the `outputs/experiments/SQL2text/` directory for further inspection and inclusion in reports or papers.

## Requirements

- Python 3.7+
- pandas, matplotlib, json_repair
- test-suite-sql-eval-master (external evaluation suite)

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Contributing

Feel free to open issues or submit pull requests to improve documentation, add features, or fix bugs.

## License

This repository is provided under the Apache License 2.0.

---

For more details, see the docstrings in each analysis script and the comments in the code.