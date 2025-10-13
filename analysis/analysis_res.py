"""Utilities for running the `test-suite-sql-eval` analysis locally.

The original version of this module primarily consisted of a monolithic
``get_accuracy`` function with a hard-coded command string. The goal of this
refactor is to make the workflow easier to follow, test, and extend by
introducing composable helper functions and lightweight configuration objects.

Typical usage from the command line::

    python -m analysis.analysis_res \
        --llms gpt-3.5-turbo gpt-4 \
        --base-path test-suite-sql-eval-master \
        --output-dir outputs/analysis

This will discover every database that has ``*_gold.txt`` files under the LLM
subdirectories and will run the evaluator, storing the stdout/stderr logs in the
``outputs/analysis`` directory.
"""

from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence


Command = List[str]
"""Convenience alias for ``subprocess`` command invocations."""


DEFAULT_LLM_MODELS: Sequence[str] = ("gpt-3.5-turbo",)
"""Default list of LLM models to evaluate when none are provided."""


@dataclass(frozen=True)
class EvaluationPaths:
    """Resolved filesystem paths used by the evaluator."""

    base_path: Path
    output_dir: Path
    evaluation_script: Path
    database_dir: Path
    tables_json: Path

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "EvaluationPaths":
        """Instantiate the dataclass from parsed CLI arguments."""

        return cls(
            base_path=args.base_path,
            output_dir=args.output_dir,
            evaluation_script=args.evaluation_script,
            database_dir=args.database_dir,
            tables_json=args.tables_json,
        )

    # -- Path helpers -----------------------------------------------------
    def examples_dir(self, llm: str) -> Path:
        """Directory containing gold/prediction files for ``llm``."""

        return self.base_path / "evaluation_examples" / llm

    def gold_file(self, llm: str, database: str) -> Path:
        """Path to the reference predictions for ``database``."""

        return self.examples_dir(llm) / f"{database}_gold.txt"

    def prediction_file(self, llm: str, database: str) -> Path:
        """Path to the model predictions for ``database``."""

        return self.examples_dir(llm) / f"{database}_predict.txt"

    def log_file(self, llm: str, database: str) -> Path:
        """Destination path for the combined stdout/stderr log."""

        return self.output_dir / f"{llm}_{database}.txt"


def discover_databases(examples_dir: Path) -> List[str]:
    """Return all database identifiers with matching ``*_gold.txt`` files."""

    suffix = "_gold.txt"
    databases = {
        file.name[: -len(suffix)]
        for file in examples_dir.glob(f"*{suffix}")
    }
    return sorted(databases)


def build_command(paths: EvaluationPaths, gold: Path, prediction: Path) -> Command:
    """Construct the evaluation command for a specific database."""

    return [
        "python3",
        str(paths.evaluation_script),
        "--gold",
        str(gold),
        "--pred",
        str(prediction),
        "--db",
        str(paths.database_dir),
        "--etype",
        "all",
        "--table",
        str(paths.tables_json),
    ]


def evaluate_database(
    llm: str,
    database: str,
    paths: EvaluationPaths,
    *,
    overwrite: bool = True,
) -> Path:
    """Run evaluation for a single database and return the log file path."""

    gold_file = paths.gold_file(llm, database)
    prediction_file = paths.prediction_file(llm, database)
    log_file = paths.log_file(llm, database)

    if not overwrite and log_file.exists():
        return log_file

    log_file.parent.mkdir(parents=True, exist_ok=True)
    command = build_command(paths, gold_file, prediction_file)

    with log_file.open("w", encoding="utf-8") as handle:
        subprocess.run(command, check=True, stdout=handle, stderr=subprocess.STDOUT)

    return log_file


def evaluate_llms(llms: Iterable[str], paths: EvaluationPaths) -> None:
    """Evaluate all provided LLMs and report progress to stdout."""

    for llm in llms:
        databases = discover_databases(paths.examples_dir(llm))
        print(f"Evaluating {llm}: found {len(databases)} database(s).")
        for database in databases:
            log_path = evaluate_database(llm, database, paths)
            print(f"  • {database} → {log_path}")


def parse_args() -> argparse.Namespace:
    """Parse command line options for the module."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--llms",
        nargs="+",
        default=DEFAULT_LLM_MODELS,
        help="Names of the LLM subdirectories under evaluation_examples",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=Path("test-suite-sql-eval-master"),
        help="Root path of the test-suite-sql-eval checkout.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/analysis"),
        help="Directory where analysis logs will be written.",
    )
    parser.add_argument(
        "--evaluation-script",
        type=Path,
        default=Path("test-suite-sql-eval-master/evaluation.py"),
        help="Path to the evaluation entrypoint script.",
    )
    parser.add_argument(
        "--database-dir",
        type=Path,
        default=Path("test-suite-sql-eval-master/database"),
        help="Path to the SQLite database directory.",
    )
    parser.add_argument(
        "--tables-json",
        type=Path,
        default=Path("test-suite-sql-eval-master/tables.json"),
        help="Location of the tables.json schema file.",
    )
    return parser.parse_args()


def main() -> None:
    """Script entrypoint used when executing the module as a program."""

    args = parse_args()
    paths = EvaluationPaths.from_args(args)
    evaluate_llms(args.llms, paths)


if __name__ == "__main__":  # pragma: no cover - CLI guard
    main()
