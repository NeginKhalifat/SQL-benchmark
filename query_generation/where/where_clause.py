import json
import random
import string

from helper_funcs import (
    generate_arithmetic_expression,
    generate_like_pattern,
    generate_random_words,
    get_attributes_ends_with,
)
from subquery_generator import generate_subquery


def basic_comparison(colms, details, random_choice):
    possible_columns = []
    if len( colms["number"]) == 1:
        possible_columns.append(colms["number"][0])
    else:
        for i in colms["number"]:
            # if "id" not in i.lower():
            possible_columns.append(i)
    # if possible_columns == []:
    #     possible_columns = colms["number"]
        
    if len(possible_columns) == 0:
        raise Exception("No number columns in basic_comparison")
    random_numeric_colm = random.choice(possible_columns)
    if details == "basic_comparison":
        print("basic_comparison")
        comparison_operators = ["=", "!=", ">", "<", ">=", "<="]
        if random_choice:
            comparison_operator = random.choice(comparison_operators)
            random_value = random.randint(1, 100)

            where_cluase = (
                    f" {random_numeric_colm} {comparison_operator} {random_value}"
                )
            return [where_cluase]
            
        else:
            where_clauses = []
            for comparison_operator in comparison_operators:
                #
                # random_exp = generate_arithmetic_expression(colms)
                # print(random_exp)
                random_value = random.randint(1, 100)

                where_cluase = (
                    f" {random_numeric_colm} {comparison_operator} {random_value}"
                )
                where_clauses.append(where_cluase)

            return where_clauses

    comparison_operator = details["basic_comparison"]
    return generate_arithmetic_value_exp_for_basic_comparison(
        colms, random_numeric_colm, comparison_operator
    )


# TODO Rename this here and in `basic_comparison`
def generate_arithmetic_value_exp_for_basic_comparison(
    colms, random_numeric_colm, comparison_operator, where_clause=False
):
    # random_exp = generate_arithmetic_expression(colms, where_clause=where_clause)
    random_value = random.randint(0,100)

    where_cluase = f" {random_numeric_colm} {comparison_operator} {random_value}"
    return [where_cluase]


def pattern_matching(colms, details, random_choice):
    if not colms["text"]:
        raise Exception("There is no text column in the schema for pattern matching")
    random_text_colm = random.choice(colms["text"])
    if details == "pattern_matching":
        pattern_matching_types = [
            "starts_with_a",
            "ends_with_ing",
            "exactly_5_characters",
            "does_not_contain_xyz",
        ]
        if random_choice:
            pattern_type = random.choice(pattern_matching_types)
            if pattern := generate_like_pattern(pattern_type):
                if random.choice([True, False]):
                    where_cluase = f"{random_text_colm} LIKE {pattern}"
                else:
                    where_cluase = f"{random_text_colm} NOT LIKE {pattern}"
                return [where_cluase]
        else:
            where_clauses = []
            for pattern_type in pattern_matching_types:
                if pattern := generate_like_pattern(pattern_type):
                    where_cluase = f"{random_text_colm} LIKE {pattern}"

                    where_clauses.append(where_cluase)
                    where_cluase = f"{random_text_colm} NOT LIKE {pattern}"
                    where_clauses.append(where_cluase)
            return [where_clauses]

    if isinstance(details["pattern_matching"], list):
        op = details["pattern_matching"][0]
        pattern_type = details["pattern_matching"][1]
        if pattern := generate_like_pattern(pattern_type):
            where_cluase = f"{random_text_colm} {op} {pattern}"
            return [where_cluase]


def null_check(colms, details, random_choice):
    for i in colms["number"]:
        if "id" not in i.lower():
            possible_columns.append(i)
    possible_columns += colms["text"]
    possible_columns += colms["time"]
    if len(possible_columns) == 0:
        raise Exception("No number, text or time columns")
    random_text_colm = random.choice(possible_columns)
    if details == "null_check":
        null_check_operators = ["IS NULL", "IS NOT NULL"]
        if random_choice:
            op = random.choice(null_check_operators)
            where_cluase = f"{random_text_colm} {op}"
            return [where_cluase]
        else:
            where_clauses = []
            for op in null_check_operators:
                where_cluase = f"{random_text_colm} {op}"
                where_clauses.append(where_cluase)
            return where_clauses
    op = details["null_check"]
    where_cluase = f"{random_text_colm} {op}"
    return [where_cluase]


def create_statement_for_number_set(colms, in_or_not_in):
    possible_columns = []
    for i in colms["number"]:
        if "id" not in i.lower():
            possible_columns.append(i)

    if len(possible_columns) == 0:
        raise Exception("No number columns")
    random_numeric_colm = random.choice(possible_columns)
    list_of_numbers = ["1", "2", "3", "40", "5"]

    return f'{random_numeric_colm} {in_or_not_in} ({", ".join(list_of_numbers)})'


def create_statement_for_text_set(colms, in_or_not_in):
    random_text_colm = random.choice(colms["text"])
    word_list = ["'apple'", "'banana'", "'orange'", "'grape'", "'pineapple'"]
    random_words = generate_random_words(word_list, 5)

    return f'{random_text_colm} {in_or_not_in} ({", ".join(random_words)})'


def in_clause(colms, details):
    if len(colms["text"]) != 0 and len(colms["number"]) != 0 :
        if random.choice([True, False]):
            where_cluase = create_statement_for_number_set(colms, "IN")
        else:
            where_cluase = create_statement_for_text_set(colms, "IN")
        return [where_cluase]
    elif len(colms["text"]) > 0:
        where_cluase = create_statement_for_text_set(colms, "IN")
        return [where_cluase]
    elif len(colms["number"]) > 0:
        where_cluase = create_statement_for_number_set(colms, "IN")
        return [where_cluase]


def not_in_clause(colms, details):
    if len(colms["text"]) == 0 and len(colms["number"]) == 0:
        if random.choice([True, False]):
            where_cluase = create_statement_for_number_set(colms, "NOT IN")
        else:
            where_cluase = create_statement_for_text_set(colms, "NOT IN")
        return [where_cluase]
    elif colms["text"]:
        where_cluase = create_statement_for_text_set(colms, "NOT IN")
        return [where_cluase]
    elif colms["number"]:
        where_cluase = create_statement_for_number_set(colms, "NOT IN")
        return [where_cluase]


def between_clause(colms):
    if colms["number"]:
        possible_columns = []
        if len(colms["number"]) == 1:
            return [f"{colms['number'][0]} BETWEEN 1 AND 10"]
        for i in colms["number"]:
            # if "id" not in i.lower():
            possible_columns.append(i)
        random_numeric_colm = random.choice(possible_columns)

        where_cluase = f"{random_numeric_colm} BETWEEN 1 AND 10"
        return [where_cluase]
    else:
        raise Exception("There is no number column in the schema")


def logical_operator(
    schema,
    schema_types,
    db_name,
    colms,
    details,
    pk,
    fk,
    tables,
    random_choice,
    create_where_clause_func=None,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    print("logical_operator")
    if "subquery" in details["logical_operator"][1]:
        print("OIO")
        return subquery(
            schema,
            schema_types,
            db_name,
            colms,
            details["logical_operator"][1],
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )
    else:
        predicator1 = create_where_clause_func(
            schema,
            schema_types,
            db_name,
            colms,
            details["logical_operator"][1],
            pk,
            fk,
            tables,
            random_choice=random_choice,
        )
    op = details["logical_operator"][0]
    if "subquery" in details["logical_operator"][2]:
        predicator2 = subquery(
            schema,
            schema_types,
            db_name,
            colms,
            details["logical_operator"][2],
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )
    else:
        predicator2 = create_where_clause(
            schema,
            schema_types,
            db_name,
            colms,
            details["logical_operator"][2],
            pk,
            fk,
            tables,
            random_choice=random_choice,
        )
    where_clauses = []
    print("Second",predicator2)
    for first_predicator in predicator1:
        for second_predicator in predicator2:
            where_cluase = f"{first_predicator} {op} {second_predicator}"
            where_clauses.append(where_cluase)
    return where_clauses


def subquery(
    schema,
    schema_types,
    db_name,
    colms,
    details,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_single_schema_func,
):
    print("subquery")
    print(details)
    return generate_subquery(
        schema,
        schema_types,
        db_name,
        colms,
        details,
        pk,
        fk,
        tables,
        min_max_depth_in_subquery=min_max_depth_in_subquery,
        query_generator_single_schema_func=query_generator_single_schema_func,
    )


def create_where_clause(
    schema,
    schema_types,
    db_name,
    colms,
    details,
    pk,
    fk,
    tables,
    random_choice=False,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    if min_max_depth_in_subquery is None:
        min_max_depth_in_subquery = [0, 0]
    if "none" in details:
        return ""
    if "basic_comparison" in details:
        return basic_comparison(colms, details, random_choice)
    if "pattern_matching" in details:
        try:
            return pattern_matching(colms, details, random_choice)
        except Exception as e:
            raise e
    if "null_check" in details:
        return null_check(colms, details, random_choice)
    if "IN" in details and "NOT IN" not in details:
        return in_clause(colms, details)
    if "NOT IN" in details:
        return not_in_clause(colms, details)
    if "between" in details:
        return between_clause(colms)
    if "logical_operator" in details:
        return logical_operator(
            schema,
            schema_types,
            db_name,
            colms,
            details,
            pk,
            fk,
            tables,
            random_choice,
            create_where_clause_func=create_where_clause,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )
    if "subquery" in details:
        return subquery(
            schema,
            schema_types,
            db_name,
            colms,
            details,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )


def complete_with_where_clause(
    schema,
    schema_types,
    db_name,
    temp_query,
    attributes,
    where_clauses_types,
    pk,
    fk,
    tables,
    must_be_in_where=None,
    random_choice=False,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    """
    Complete the given query with a WHERE clause based on the provided parameters.

    Args:
        schema (dict): Dictionary containing the schema information.
        schema_types (dict): Dictionary containing the schema types information.
        db_name (str): Name of the database.
        temp_query (str): Temporary query to be completed with the WHERE clause.
        attributes (dict): Dictionary containing the attributes, with keys "number" and "text" representing different types.
        where_clauses_types (list): List of where clause types to choose from.
        pk (dict): Dictionary mapping table names to their primary key columns.
        fk (list): List of join definitions, each containing "table1", "table2", "first_key", and "second_key".
        tables (list): List of table names.
        must_be_in_where (list, optional): List containing the must-be-in-where condition. Defaults to None.
        random_choice (bool, optional): Flag indicating whether to use random choice for where clause generation. Defaults to False.
        min_max_depth_in_subquery (list, optional): List containing the minimum and maximum depth of nested subqueries. Defaults to [0, 0].
        query_generator_single_schema_func (function, optional): Function to generate queries. Defaults to None.

    Returns:
        list: List of completed queries with their attributes.

    Raises:
        Exception: If an error occurs during query completion.

    Examples:
        >>> schema = {...}
        >>> schema_types = {...}
        >>> db_name = "mydb"
        >>> temp_query = "SELECT * FROM table"
        >>> attributes = {"number": ["col1", "col2"], "text": ["col3", "col4"]}
        >>> where_clauses_types = ["type1", "type2"]
        >>> pk = {"table": "pk"}
        >>> fk = [{"table1": "table1", "table2": "table2", "first_key": "fk1", "second_key": "pk1"}]
        >>> tables = ["table1", "table2"]
        >>> complete_with_where_clause(schema, schema_types, db_name, temp_query, attributes, where_clauses_types, pk, fk, tables)
        [['SELECT * FROM table WHERE ...', {...}]]
    """
  
    if min_max_depth_in_subquery is None:
        min_max_depth_in_subquery = [0, 0]
    try:
        where_clause = create_where_clause(
            schema,
            schema_types,
            db_name,
            attributes,
            where_clauses_types,
            pk,
            fk,
            tables,
            random_choice=random_choice,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )
        print("complete_with_where_clause")
        print(where_clause)

        if where_clause == "":
            return (
                [
                    [
                        f"{temp_query} WHERE {must_be_in_where[0]}{get_attributes_ends_with(must_be_in_where[1], attributes)}",
                        attributes,
                    ]
                ]
                if must_be_in_where
                else [[temp_query, attributes]]
            )

        if isinstance(where_clause, list):
            queries = []
            for where in where_clause:
                
                query = (
                    f"{temp_query} WHERE {where} AND {must_be_in_where[0]}{get_attributes_ends_with(must_be_in_where[1], attributes)}"
                    if must_be_in_where
                    else f"{temp_query} WHERE {where}"
                )
                queries.append([query, attributes])

            return queries

    except Exception as e:
        raise e


schema = {
    "city": [
        "city_id",
        "official_name",
        "status",
        "area_km_2",
        "population",
        "census_ranking",
    ],
    "farm": [
        "farm_id",
        "year",
        "total_horses",
        "working_horses",
        "total_cattle",
        "oxen",
        "bulls",
        "cows",
        "pigs",
        "sheep_and_goats",
    ],
    "farm_competition": ["competition_id", "year", "theme", "host_city_id", "hosts"],
    "competition_record": ["competition_id", "farm_id", "rank"],
}
schema_types = {
    "city": {
        "city_id": "number",
        "official_name": "text",
        "status": "text",
        "area_km_2": "number",
        "population": "number",
        "census_ranking": "text",
    },
    "farm": {
        "farm_id": "number",
        "year": "number",
        "total_horses": "number",
        "working_horses": "number",
        "total_cattle": "number",
        "oxen": "number",
        "bulls": "number",
        "cows": "number",
        "pigs": "number",
        "sheep_and_goats": "number",
    },
    "farm_competition": {
        "competition_id": "number",
        "year": "number",
        "theme": "text",
        "host_city_id": "number",
        "hosts": "text",
    },
    "competition_record": {
        "competition_id": "number",
        "farm_id": "number",
        "rank": "number",
    },
}

temp_query = "FROM city JOIN competition_record JOIN farm JOIN farm_competition ON farm_competition.host_city_id = city.city_id AND competition_record.farm_id = farm.farm_id AND competition_record.competition_id = farm_competition.competition_id"

# all_columns = all_colms(
#     schema,
#     schema_types,
#     ["city", "competition_record", "farm", "farm_competition"],
# )
# create_where_clause(
#     all_columns,
#     [
#         "basic_comparison",
#         "pattern_matching",
#         "null_check",
#         "in",
#         "between",
#         "logical_operators",
#     ],
# )
# complete_queries(
#     temp_query,
#     ["city", "competition_record", "farm", "farm_competition"],
#     schema,
#     schema_types,
# )
