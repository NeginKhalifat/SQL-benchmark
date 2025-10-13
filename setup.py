from pathlib import Path

from setuptools import find_packages, setup


BASE_DIR = Path(__file__).parent


setup(
    name="sql-benchmark",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    description="Toolkit for generating, parsing, and evaluating text-to-SQL benchmarks",
    long_description=(BASE_DIR / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
)


