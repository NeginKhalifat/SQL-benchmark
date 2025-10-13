# Spider SQL Statistics Utility

## Overview

This directory contains a Python script (`statistics.py`) for analyzing the structure and complexity of SQL queries in the Spider dataset.  
It computes detailed statistics for various SQL clauses and constructs, helping researchers understand the distribution and frequency of SQL features in the dataset.

## Features

- Loads SQL queries from a JSON file (e.g., `train_spider.json`).
- Counts and averages for:
  - Number of `SELECT` statements per query
  - Number and types of tables in `FROM` clauses (including subqueries)
  - Number and types of conditions in `WHERE` clauses (logical operators, `BETWEEN`, `IN`, `LIKE`, `EXISTS`, subqueries, `NULL`)
  - Number of column names in `GROUP BY` clauses
  - Number and types of `HAVING` clauses
  - Number and details of `ORDER BY` clauses (including multiple orderings)
  - Number of queries with `LIMIT` clauses
  - Number of queries with set operations: `INTERSECT`, `UNION`, `EXCEPT`
- Calculates averages for each clause type across all queries.
- Prints a summary of all statistics for easy inspection.

## Usage

### Prerequisites

- Python 3.x

### Input

- The script expects a JSON file (e.g., `train_spider.json`) containing a list of queries, each with a parsed SQL structure under the `"sql"` key and the original query under the `"query"` key.

### Running the Script

Place your Spider dataset JSON file in the same directory and run:

```bash
python statistics_1.py
```

By default, it loads `train_spider.json`.  
You can modify the filename in the script if needed.

### Output

The script prints statistics and averages for each SQL clause, including:

- SELECT statements per query
- FROM clause details (number of tables, subqueries)
- WHERE clause details (logical ops, subqueries, special conditions)
- GROUP BY, HAVING, ORDER BY, LIMIT, INTERSECT, UNION, EXCEPT clause statistics

Example output:

```
--------------------SELECT STATEMENTS--------------------
Number of SELECT statements per query: {1: 5000, 2: 300, ...}
Average number of SELECT statements per query: {1: 0.92, 2: 0.06, ...}
...
-----------------------FROM CLAUSE-----------------------
FROM details: {1: 4000, "subquery": 200, ...}
Average number of FROM clause: {1: 0.85, "subquery": 0.04, ...}
...
-----------------------WHERE CLAUSE----------------------
WHERE details: {'logical_op': 1200, 'between': 100, ...}
number of where clause {1: 3500, 2: 800, ...}
Average number of WHERE clauses per query: {...}
...
```

## Customization

- To analyze a different dataset, change the filename in the script (`train_spider.json`) to your desired JSON file.
- You can extend the script to collect additional statistics as needed.
