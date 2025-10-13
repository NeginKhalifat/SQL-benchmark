"""Utilities for aggregating evaluation outputs produced by text-to-SQL runs."""

from pathlib import Path
import subprocess
from typing import Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_SUITE_PATH = REPO_ROOT / "test-suite-sql-eval-master"
EVALUATION_EXAMPLES_PATH = TEST_SUITE_PATH / "evaluation_examples"
OUTPUTS_PATH = REPO_ROOT / "outputs"


def _discover_databases(llm: str) -> List[str]:
    """Return the set of databases with gold predictions for a specific LLM."""
    llm_dir = EVALUATION_EXAMPLES_PATH / llm
    if not llm_dir.exists():
        return []

    db_names = set()
    for candidate in llm_dir.glob("*_gold.txt"):
        name = candidate.stem.replace("_gold", "")
        if name:
            db_names.add(name)
    return sorted(db_names)


def run_evaluations(llms: Iterable[str] | None = None) -> None:
    """Execute the official Spider evaluation script for each LLM/database pair."""
    llms = list(llms or ["gpt-3.5-turbo"])
    analysis_dir = OUTPUTS_PATH / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    evaluation_script = TEST_SUITE_PATH / "evaluation.py"
    gold_file = EVALUATION_EXAMPLES_PATH / "llm_based_gold.txt"
    predictions_file = EVALUATION_EXAMPLES_PATH / "llm_based_gpt_4_turbo_res.txt"
    database_root = TEST_SUITE_PATH / "database"
    tables_file = TEST_SUITE_PATH / "tables.json"

    for llm in llms:
        print(f"Processing {llm}")
        databases = _discover_databases(llm)
        print("Databases:", databases)

        for database in databases:
            output_path = analysis_dir / f"{llm}_{database}.txt"
            command = [
                "python3",
                str(evaluation_script),
                "--gold",
                str(gold_file),
                "--pred",
                str(predictions_file),
                "--db",
                str(database_root),
                "--etype",
                "all",
                "--table",
                str(tables_file),
            ]
            with output_path.open("w", encoding="utf-8") as handle:
                subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=False)


if __name__ == "__main__":
    run_evaluations()
