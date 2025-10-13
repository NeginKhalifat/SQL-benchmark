# SQL Parser Utility (`parse_sql_one.py`)

## Overview

This directory contains an improved SQL parser utility, implemented in Python, for converting SQL queries into a structured dictionary format.  
**This is an enhanced version of the original `process_sql.py` from the [test-suite-sql-eval repository](https://github.com/taoyds/test-suite-sql-eval/blob/master/process_sql.py).**  
It is designed for academic and benchmarking purposes, supporting a subset of SQL syntax with specific assumptions and features.

## Supported Features

- **Assumptions:**
  1. Input SQL queries are syntactically correct.
  2. Only table names can have aliases.
  3. Only one `INTERSECT`, `UNION`, or `EXCEPT` clause per query.

- **SQL Clauses Supported:**
  - `SELECT` (with optional `DISTINCT` and aggregation: `MAX`, `MIN`, `COUNT`, `SUM`, `AVG`)
  - `FROM` (supports table aliases and subqueries as tables)
  - `WHERE` (logical operators, comparison operators, `BETWEEN`, `IN`, `LIKE`, `IS`, `EXISTS`)
  - `GROUP BY`
  - `ORDER BY` (supports multiple clauses with different modes: `ASC`, `DESC`; supports using numbers as select clause values, e.g., `ORDER BY 1 DESC`)
  - `HAVING`
  - `LIMIT`
  - Set operations: `INTERSECT`, `UNION`, `EXCEPT` (only one per query)

- **Joins:**
  - Explicit `JOIN` and `ON` clauses in the `FROM` section.

- **Subqueries:**
  - Subqueries in `SELECT`, `FROM`, and `WHERE` clauses.
  - Subqueries as values in conditions, e.g., `t.cost = (SELECT ...)`.
  - Subqueries as tables in the `FROM` clause.

- **Alias Handling:**
  - Explicit (`AS`) and implicit table aliases.

- **Schema Mapping:**
  - Maps table and column names to unique identifiers using the database schema.

- **Additional Supported Syntax:**
  - Composite column names (e.g., `table.column`).
  - Numeric tokens as columns (e.g., `SELECT 1`).
  - Multiple values connected with `AND`/`OR` in conditions.
  - `EXISTS` as a condition.

## File Structure

- `parse_sql_one.py`: Main parser implementation.
- Example schema and database files should be placed in the appropriate directories (see usage).

## Usage

### Prerequisites

- Python 3.7+
- `nltk` (for tokenization)
- `sqlite3` (standard library)
- Example SQLite database and schema JSON file

Install NLTK if not already installed:

```bash
pip install nltk
```

### Example Usage

```python
from parse_sql_one import Schema, get_schema, get_sql

db_path = "test-suite-sql-eval-master/database/twitter_1/twitter_1.sqlite"
schema = Schema(get_schema(db_path))
sql_query = "SELECT City, Hanzi FROM city AS hosting_city ORDER BY City ASC"
parsed_sql = get_sql(schema, sql_query)
print(parsed_sql)
```

Or run the script directly:

```bash
python parse_sql_one.py
```

## Output Format

The parser outputs a nested dictionary representing the SQL query structure, including all supported clauses and set operations. Example output:

```python
{
  'select': (isDistinct, [(agg_id, val_unit), ...]),
  'from': {'table_units': [...], 'conds': [...]},
  'where': [...],
  'groupBy': [...],
  'orderBy': ('asc'/'desc', [...]),
  'having': [...],
  'limit': value,
  'intersect': None/sql,
  'except': None/sql,
  'union': None/sql
}
```

## Limitations

- Only supports one set operation (`INTERSECT`, `UNION`, or `EXCEPT`) per query.
- Assumes SQL queries are correct and well-formed.
- Only table names can have aliases; column aliases are not supported.
- Designed for academic and benchmarking use, not for production SQL parsing.

## Reference

- Original version: [`process_sql.py` from test-suite-sql-eval](https://github.com/taoyds/test-suite-sql-eval/blob/master/process_sql.py)

For questions or suggestions, please open an issue or contact the repository
