# Module map

The Python source tree lives under [`src/sql_benchmark`](../src/sql_benchmark). The table below summarises the purpose of each top-level subpackage.

| Module | Highlights |
| --- | --- |
| `sql_benchmark.analysis` | Post-processing utilities for summarising evaluation results, generating plots, and collating statistics. The modules now resolve dataset paths via `Path` objects so they can be executed from anywhere within the repository. |
| `sql_benchmark.parser_sql` | Low-level SQL parsers built around the Spider evaluation toolkit (`parse_sql_one.py` and helpers). These modules are imported by both query generation and evaluation routines. |
| `sql_benchmark.query_generation` | Specification builders, helper utilities, and orchestration code for creating synthetic SQL queries. Outputs are written to `data/processed/synthetic_queries`. |
| `sql_benchmark.sql2natsql` | Bridge utilities for converting SQL to the NatSQL representation. |
| `sql_benchmark.sql2text` | SQL-to-natural-language conversion code (rule-based and back-translation approaches). Includes join relationship extraction helpers that consume the processed data artefacts. |
| `sql_benchmark.text_to_sql` | Entry points for text-to-SQL evaluation with different LLM backends (`gpt`, `llama`, `gemini`, `dinsql`). The modules now import `sql_benchmark.query_generation` directly from the package namespace. |

## Package expectations

- Subpackages are discoverable via `find_packages(where="src")` in `setup.py`.
- Relative paths should use `Path(__file__)` helpers rather than hard-coded working-directory assumptions.
- New utilities should include short docstrings and prefer returning structured data over printing side effects.

Refer to [`docs/workflows/synthesis.md`](workflows/synthesis.md) and [`docs/workflows/evaluation.md`](workflows/evaluation.md) for practical examples that combine modules across the package.
