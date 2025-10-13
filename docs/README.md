# Documentation index

This folder aggregates process-oriented documentation for the reorganised repository.

## Contents

| File | Summary |
| --- | --- |
| [`getting-started.md`](getting-started.md) | Step-by-step tutorial that installs dependencies, prepares datasets, and runs an end-to-end synthesis job. |
| [`data-guide.md`](data-guide.md) | Reference for the datasets, their provenance, and how intermediate files are created. |
| [`module-map.md`](module-map.md) | Overview of the Python package layout introduced by the `src/sql_benchmark` namespace. |
| [`workflows/evaluation.md`](workflows/evaluation.md) | Instructions for running evaluation pipelines and interpreting outputs. |
| [`workflows/synthesis.md`](workflows/synthesis.md) | Suggestions for orchestrating query-generation experiments with the tooling in `tools/`. |

## Conventions

- Paths are written relative to the repository root.
- Commands assume a Unix-like environment with `python`, `pip`, and `bash` available.
- The package should be installed in editable mode (`pip install -e .`) to ensure imports resolve correctly.
