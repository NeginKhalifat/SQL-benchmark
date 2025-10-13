import itertools
import os
import random
import sys

import networkx as nx

sys.path.append("..")

from helper_funcs import all_colms
from join import generate_join_query, generate_meaningless_join
from subquery_generator import generate_subquery


def handle_table_expression_for_CTE(
    schema,
    schema_types,
    db_name,
    attributes,
    table_exp_types,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    having=False,
):
    if table_exp_types == "CTE":
        subquery = generate_subquery(
            schema,
            schema_types,
            db_name,
            attributes,
            table_exp_types,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
            having=False,
        )

        return subquery[0]


def handle_table_expression_for_subquery(
    case, schema, schema_types, random_choice, CTE=""
):
    """
    Handle the table expression for subquery case.

    Args:
        case (dict): The case specifying the table expression.
        schema (dict): Database schema with table names and their columns.
        schema_types (dict): Data types of columns in the schema.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        >>> schema = {
        ...     "city": ["city_id", "name"],
        ...     "country": ["country_id", "name"],
        ... }
        >>> schema_types = {
        ...     "city": {"city_id": "int", "name": "str"},
        ...     "country": {"country_id": "int", "name": "str"},
        ... }
        >>> case = {"single_table": "city"}
        >>> handle_table_expression_for_subquery(case, schema, schema_types, random_choice=False)
        [[' FROM city', ['city'], ['city_id', 'name']]]

        >>> case = {"single_table_with_name_changing": "country"}
        >>> handle_table_expression_for_subquery(case, schema, schema_types, random_choice=False)
        [[' FROM country AS a', {'a': 'country'}, ['a.country_id', 'a.name']]]
    """
    type_of_table = list(case.keys())[0]

    table = case[type_of_table]

    queries_with_attributes = []

    if type_of_table == "single_table":
        table_expression_query = f" FROM {table}"
        attributes = all_colms(schema, schema_types, [table])
        queries_with_attributes.append(
            [table_expression_query, [table], attributes, CTE]
        )
    elif type_of_table == "single_table_with_name_changing":
        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
        table_expression_query = f" FROM {table} AS {alias_name}"
        attributes = all_colms(schema, schema_types, [table], [alias_name])
        queries_with_attributes.append(
            [table_expression_query, {alias_name: table}, attributes, CTE]
        )
    else:
        table_expression_query = f" FROM {type_of_table}"
        attributes = all_colms(schema, schema_types, [type_of_table])
        queries_with_attributes.append([table_expression_query, case, attributes, CTE])

    return queries_with_attributes


def handle_single_table_case(schema, schema_types, random_choice, CTE=""):
    """
    Handle the single table case.

    Args:
        schema (dict): Database schema with table names and their columns.
        schema_types (dict): Data types of columns in the schema.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        >>> schema = {
        ...     "city": ["city_id", "name"],
        ...     "country": ["country_id", "name"],
        ... }
        >>> schema_types = {
        ...     "city": {"city_id": "int", "name": "str"},
        ...     "country": {"country_id": "int", "name": "str"},
        ... }
        >>> handle_single_table_case(schema, schema_types, random_choice=False)
        [[' FROM city', ['city'], ['city_id', 'name']], [' FROM country', ['country'], ['country_id', 'name']]]
    """
    queries_with_attributes = []

    if random_choice:
        table = random.choice(list(schema.keys()))
        table_expression_query = f" FROM {table}"
        attributes = all_colms(schema, schema_types, [table])
        queries_with_attributes.append(
            [table_expression_query, [table], attributes, CTE]
        )
    else:
        for table in schema:
            table_expression_query = f" FROM {table}"
            attributes = all_colms(schema, schema_types, [table])
            queries_with_attributes.append(
                [table_expression_query, [table], attributes, CTE]
            )

    return queries_with_attributes


def handle_single_table_with_name_changing_case(
    schema, schema_types, random_choice, CTE=""
):
    """
    Handle the single table with name changing case.

    Args:
        schema (dict): Database schema with table names and their columns.
        schema_types (dict): Data types of columns in the schema.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        >>> schema = {
        ...     "city": ["city_id", "name"],
        ...     "country": ["country_id", "name"],
        ... }
        >>> schema_types = {
        ...     "city": {"city_id": "int", "name": "str"},
        ...     "country": {"country_id": "int", "name": "str"},
        ... }
        >>> handle_single_table_with_name_changing_case(schema, schema_types, random_choice=False)
        [[' FROM city AS a', {'a': 'city'}, ['a.city_id', 'a.name']], [' FROM country AS b', {'b': 'country'}, ['b.country_id', 'b.name']]]

    """
    queries_with_attributes = []

    if random_choice:
        table = random.choice(list(schema.keys()))
        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
        table_expression_query = f" FROM {table} AS {alias_name}"
        attributes = all_colms(schema, schema_types, [table], [alias_name])
        queries_with_attributes.append(
            [table_expression_query, [{alias_name: table}], attributes, CTE]
        )
    else:
        for table in schema:
            alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")
            table_expression_query = f" FROM {table} AS {alias_name}"
            attributes = all_colms(schema, schema_types, [table], [alias_name])
            queries_with_attributes.append(
                [table_expression_query, [{alias_name: table}], attributes, CTE]
            )

    return queries_with_attributes


def handle_join_case(
    case, schema, fk, schema_types, meaningful_joins, pk, random_choice, CTE=""
):
    """
    Handle the join case.

    Args:
        case (str): The case specifying the table expression.
        schema (dict): Database schema with table names and their columns.
        fk (dict): Foreign keys for tables.
        schema_types (dict): Data types of columns in the schema.
        meaningful_joins (str): Type of join - meaningful or meaningless.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        Examples:
        >>> schema = {
        ...     "city": [
        ...         "city_id",
        ...         "official_name",
        ...         "status",
        ...         "area_km_2",
        ...         "population",
        ...         "census_ranking",
        ...     ],
        ...     "farm": [
        ...         "farm_id",
        ...         "year",
        ...         "total_horses",
        ...         "working_horses",
        ...         "total_cattle",
        ...         "oxen",
        ...         "bulls",
        ...         "cows",
        ...         "pigs",
        ...         "sheep_and_goats",
        ...     ],
        ...     "farm_competition": ["Competition_ID", "Year", "Theme", "Host_city_ID", "Hosts"],
                "competition_record": ["Competition_ID", "Farm_ID", "Rank"],
        ... }
        >>> fk = {
        ...     "farm_competition": {"host_city_id": ("city", "city_id")},
        ...     # Other foreign keys...
        ... }
        >>> schema_types = {
        ...     "city": {
        ...         "city_id": "number",
        ...         "official_name": "text",
        ...         # Other column types...
        ...     },
        ...     "farm": {
        ...         "farm_id": "number",
        ...         "year": "number",
        ...         # Other column types...
        ...     },
        ...     # Other tables and column types...
        ... }
        >>> handle_join_case("INNER_JOIN", schema, fk, schema_types, "yes", random_choice=False)
        [[FROM city INNER JOIN farm_competition ON city.city_id = farm_competition.host_city_id', ['city', 'farm_competition']]

    """
    queries_with_attributes = []

    num_joins = len(case.split("_"))

    join_types = case.split("_")

    if meaningful_joins == "yes":
        queries_with_attributes = handle_meaningful_joins(
            schema, fk, join_types, schema_types, pk, random_choice, CTE
        )
    elif meaningful_joins == "no":
        queries_with_attributes = handle_meaningless_joins(
            schema, num_joins, join_types, schema_types, random_choice, CTE
        )
    elif random_choice:
        meaningful_joins = "no"
        queries_with_attributes = handle_meaningless_joins(
            schema, num_joins, join_types, schema_types, random_choice, CTE
        )
    else:
        queries_with_attributes = handle_meaningless_joins(
            schema, num_joins, join_types, schema_types, random_choice, CTE
        )

        temp_queries = generate_join_query(
            schema, fk, join_types, pk, random_choice=random_choice, CTE=CTE
        )
        for temp in temp_queries:
            all_columns = all_colms(schema, schema_types, temp[1])
            temp.append(all_columns)
            temp.append(CTE)
            queries_with_attributes.append(temp)
    return queries_with_attributes


def handle_meaningful_joins(
    schema, fk, join_types, schema_types, pk, random_choice, CTE=""
):
    """
    Handle the meaningful joins.

    Args:
        schema (dict): Database schema with table names and their columns.
        fk (dict): Foreign keys for tables.
        join_types (list): List of join types.
        schema_types (dict): Data types of columns in the schema.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        >>> schema = {
        ...     "city": [
        ...         "city_id",
        ...         "official_name",
        ...         "status",
        ...         "area_km_2",
        ...         "population",
        ...         "census_ranking",
        ...     ],
        ...     "farm": [
        ...         "farm_id",
        ...         "year",
        ...         "total_horses",
        ...         "working_horses",
        ...         "total_cattle",
        ...         "oxen",
        ...         "bulls",
        ...         "cows",
        ...         "pigs",
        ...         "sheep_and_goats",
        ...     ],
        ...     # Other tables and columns...
        ... }
        >>> fk = {
        ...     "farm_competition": {"host_city_id": ("city", "city_id")},
        ...     # Other foreign keys...
        ... }
        >>> schema_types = {
        ...     "city": {
        ...         "city_id": "number",
        ...         "official_name": "text",
        ...         # Other column types...
        ...     },
        ...     "farm": {
        ...         "farm_id": "number",
        ...         "year": "number",
        ...         # Other column types...
        ...     },
        ...     # Other tables and column types...
        ... }
        >>> join_types = ["INNER JOIN", "LEFT JOIN"]
        >>> handle_meaningful_joins(schema, fk, join_types, schema_types, random_choice=False)
        [[' FROM city INNER JOIN farm_competition ON city.city_id = farm_competition.host_city_id', ['city', 'farm_competition']]]

    """
    print("HI))))))))))))))))))))")
    temp_queries = generate_join_query(
        schema, fk, join_types, pk, random_choice=random_choice
    )
    print("BYEEE")
    queries_with_attributes = []
    for temp in temp_queries:
        print("temp", temp)
        all_columns = all_colms(schema, schema_types, temp[1])
        print("all_columns", all_columns)
        temp.append(all_columns)
        temp.append(CTE)
        queries_with_attributes.append(temp)
    return queries_with_attributes


def handle_meaningless_joins(
    schema, num_joins, join_types, schema_types, random_choice, CTE=""
):
    """
    Handle the meaningless joins.

    Args:
        schema (dict): Database schema with table names and their columns.
        num_joins (int): Number of joins to generate.
        join_types (list): List of join types.
        schema_types (dict): Data types of columns in the schema.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: List of queries with attributes based on the specified table expression type.

    Examples:
        >>> schema = {
        ...     "city": [
        ...         "city_id",
        ...         "official_name",
        ...         "status",
        ...         "area_km_2",
        ...         "population",
        ...         "census_ranking",
        ...     ],
        ...     "farm": [
        ...         "farm_id",
        ...         "year",
        ...         "total_horses",
        ...         "working_horses",
        ...         "total_cattle",
        ...         "oxen",
        ...         "bulls",
        ...         "cows",
        ...         "pigs",
        ...         "sheep_and_goats",
        ...     ],
        ...     # Other tables and columns...
        ... }
        >>> schema_types = {
        ...     "city": {
        ...         "city_id": "number",
        ...         "official_name": "text",
        ...         # Other column types...
        ...     },
        ...     "farm": {
        ...         "farm_id": "number",
        ...         "year": "number",
        ...         # Other column types...
        ...     },
        ...     # Other tables and column types...
        ... }
        >>> num_joins = 2
        >>> join_types = ["INNER JOIN", "LEFT JOIN"]
        >>> handle_meaningless_joins(schema, num_joins, join_types, schema_types, random_choice=False)
        [[' FROM city INNER JOIN farm ON city.city_id = farm.farm_id', ['city', 'farm']]]

    """
    temp_queries = generate_meaningless_join(
        schema, num_joins, join_types, random_choice=random_choice
    )
    queries_with_attributes = []
    for temp in temp_queries:
        all_columns = all_colms(schema, schema_types, temp[1])
        queries_with_attributes.append([temp[0], temp[1], all_columns, CTE])
    return queries_with_attributes
