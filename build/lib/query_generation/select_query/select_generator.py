import random

from helper_funcs import generate_random_alias_name
from subquery_generator import generate_subquery

from .select_helper_funcs import (
    handle_agg_exp,
    handle_arithmatic_exp,
    handle_count_distinct_exp,
    handle_single_exp,
    handle_string_func_exp,
)


def complete_query_with_select(
    schema,
    schema_types,
    db_name,
    pk,
    fk,
    tables,
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    is_subquery=False,
    random_choice=False,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    cte="",
    rename_must_be_in_select=False,
):
    """
    Completes the query with the SELECT clause based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        is_subquery (bool, optional): Indicates if it's a subquery. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """

    if cte != "":
        cte = cte[0]

    select_clauses = generate_select_clause(
        schema,
        schema_types,
        db_name,
        pk,
        fk,
        tables,
        temp_query,
        attributes,
        must_have_attributes,
        select_statement_type,
        distinct_type,
        is_subquery=is_subquery,
        random_choice=random_choice,
        min_max_depth_in_subquery=min_max_depth_in_subquery,
        query_generator_single_schema_func=query_generator_single_schema_func,
        rename_must_be_in_select=rename_must_be_in_select,
    )
    return [
        [
            cte + select_statement + temp_query,
            attributes,
            must_have_attributes,
            select_fields,
            num_value_exps,
            select_fields_types,
        ]
        for select_statement, select_fields, num_value_exps, select_fields_types in select_clauses
    ]


def generate_select_clause(
    schema,
    schema_types,
    db_name,
    pk,
    fk,
    tables,
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    has_group_by=None,
    num_columns=None,
    is_subquery=False,
    random_choice=False,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    rename_must_be_in_select=False,
):
    """
    Generates the SELECT clause based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        has_group_by (bool, optional): Indicates if the query has a GROUP BY clause. Defaults to None.
        num_columns (int, optional): The number of columns. Defaults to None.
        is_subquery (bool, optional): Indicates if it's a subquery. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """
    print("SELECT STATEMENT TYPE", select_statement_type)

    if select_statement_type == "*":
        return [
            [
                "SELECT * ",
                attributes["number"] + attributes["text"] + attributes["time"],
                len(attributes["number"] + attributes["text"] + attributes["time"]),
                {},
            ]
        ]

    if has_group_by := has_group_by is not False and "GROUP BY" in temp_query:
        return generate_select_clause_with_group_by(
            schema,
            schema_types,
            db_name,
            pk,
            fk,
            tables,
            temp_query,
            attributes,
            must_have_attributes,
            select_statement_type,
            distinct_type,
            random_choice=random_choice,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
            rename_must_be_in_select=rename_must_be_in_select,
        )

    elif is_subquery:
        print("IS SUBQUERY")
        return generate_select_clause_subquery(
            must_have_attributes,
            select_statement_type,
            attributes,
        )

    else:
        return generate_value_expressions(
            schema,
            schema_types,
            db_name,
            pk,
            fk,
            tables,
            must_have_attributes,
            select_statement_type,
            attributes,
            distinct_type,
            random_choice,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )


def generate_select_clause_with_group_by(
    schema,
    schema_types,
    db_name,
    pk,
    fk,
    tables,
    temp_query,
    attributes,
    must_have_attributes,
    select_statement_type,
    distinct_type,
    random_choice=False,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    rename_must_be_in_select=False,
):
    """
    Generates the SELECT clause with GROUP BY based on the provided parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): The attributes dictionary.
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        distinct_type (str): The distinct type.
        has_group_by (bool, optional): Indicates if the query has a GROUP BY clause. Defaults to False.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: The list of select clauses.
    """
    select_statement_with_fields = generate_select_clause(
        schema,
        schema_types,
        db_name,
        pk,
        fk,
        tables,
        temp_query,
        attributes,
        must_have_attributes,
        select_statement_type,
        distinct_type,
        has_group_by=False,
        random_choice=random_choice,
        min_max_depth_in_subquery=min_max_depth_in_subquery,
        query_generator_single_schema_func=query_generator_single_schema_func,
    )
    queries = []
    for temp in select_statement_with_fields:
        select_statement = temp[0]
        select_fields_temp = temp[1]

        select_fields = []
        num_value_exps = len(select_fields_temp) + len(select_statement_type)
        select_fields += select_fields_temp
        select_statement += ", "
        if rename_must_be_in_select:

            for attribute in must_have_attributes:
                alias_suffix = random.choice("abcdefghijklmnopqrstuvwxyz")
                select_statement += f"{attribute} AS {attribute}_{alias_suffix}, "
                select_fields.append(f"{attribute}_{alias_suffix}")
        else:
            for attribute in must_have_attributes:
                if attribute not in select_fields:
                    select_statement += f"{attribute}, "
                    select_fields.append(attribute)
            # select_statement += ", ".join(must_have_attributes)

    if select_statement.endswith(", "):
        select_statement = select_statement[:-2]
    queries.append([select_statement, select_fields, num_value_exps, temp[3]])

    return queries


def generate_select_clause_subquery(
    must_have_attributes,
    select_statement_type,
    attributes,
):
    """
    Generates the SELECT clause for a subquery based on the provided parameters.

    Args:
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        attributes (dict): The attributes dictionary.

    Returns:
        list: The list of select clauses.
    """
    select_statement = "SELECT " + ", ".join(must_have_attributes)
    select_fields = must_have_attributes.copy()
    num_value_exps = len(select_fields)
    return [[select_statement, select_fields, num_value_exps, {}]]


def generate_value_expressions(
    schema,
    schema_types,
    db_name,
    pk,
    fk,
    tables,
    must_have_attributes,
    select_statement_type,
    attributes,
    distinct_type,
    random_choice,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    """
    Generates the SELECT statements based on the provided parameters.

    Args:
        must_have_attributes (list): The list of must-have attributes.
        select_statement_type (str or list): The select statement type.
        attributes (dict): The attributes dictionary.
        distinct_type (str): The distinct type.
        random_choice (bool): Indicates if random choice is enabled.

    Returns:
        list: The list of select statements.
    """

    select_fields_types = {}
    select_statements = []  # List to store the generated SELECT statements
    repeat_num = (
        1 if random_choice else 3
    )  # Number of times to repeat the generation process
    for _ in range(repeat_num):
        num_value_exps = 0  # Number of value expressions

        select_statement = "SELECT "
        if distinct_type == "distinct":
            select_statement += (
                "DISTINCT "  # Add DISTINCT keyword if distinct_type is "distinct"
            )
        select_fields = []  # List to store the select fields

        for col_type in select_statement_type:
            num_value_exp = 0  # Number of value expressions
            # TODO
            print("ATTRIBUTES", attributes)
             # Select a random column
            possible_columns = attributes["text"] + attributes["time"]
            for i in attributes["number"]:
                # if "id" not in i.lower():
                possible_columns.append(i)
           
            random_column = random.choice(
               possible_columns
            ) 
            print("RANDOM COLUMN", random_column)
            print(possible_columns)
            if col_type.startswith("single_exp"):
                number_or_text_or_time = col_type.split("_")[2]
                try:
                    select_statement, select_fields = handle_single_exp(
                        select_statement,
                        select_fields,
                        number_or_text_or_time,
                        attributes,
                    )
                    print("SELECT STATEMENT", select_statement)
                    print("SELECT FIELDS", select_fields)
                except Exception as e:
                    print(e)
                    raise e

                num_value_exp = 1  # Increment the number of value expressions

            elif col_type == "alias_exp":
                alias_name = generate_random_alias_name(
                    select_fields, random_column
                )  # Generate a random alias name
                select_statement += f"{random_column} AS {alias_name}, "  # Add the column with alias to the SELECT statement
                select_fields.append(
                    alias_name
                )  # Add the alias to the select_fields list
                if random_column in attributes["number"]:
                    select_fields_types[alias_name] = "number"
                elif random_column in attributes["text"]:
                    select_fields_types[alias_name] = "text"
                else:
                    select_fields_types[alias_name] = "time"
                
                num_value_exp = 1  # Increment the number of value expressions

            elif col_type.startswith("arithmatic_exp"):
                print("ARITHMATIC EXP")
                select_statement, select_fields, num_value_exp = handle_arithmatic_exp(
                    select_statement,
                    select_fields,
                    col_type,
                    random_column,
                    attributes,
                    select_fields_types,
                )  # Handle arithmetic expression and update select_statement and select_fields

            elif col_type.startswith("string_func_exp"):
                try:
                    (
                        select_statement,
                        select_fields,
                        num_value_exp,
                    ) = handle_string_func_exp(
                        select_statement,
                        select_fields,
                        col_type,
                        random_column,
                        attributes,
                        select_fields_types,
                    )  # Handle string function expression and update select_statement and select_fields
                except Exception as e:
                    print(e)
                    raise e
            elif col_type.startswith("agg_exp"):
                select_statement, select_fields, num_value_exp = handle_agg_exp(
                    select_statement,
                    select_fields,
                    col_type,
                    random_column,
                    attributes,
                    select_fields_types,
                )  # Handle aggregate expression and update select_statement and select_fields
            elif col_type.startswith("count_distinct_exp"):
                (
                    select_statement,
                    select_fields,
                    num_value_exp,
                ) = handle_count_distinct_exp(
                    select_statement,
                    select_fields,
                    col_type,
                    random_column,
                    attributes,
                    select_fields_types,
                )  # Handle count distinct expression and update select_statement and select_fields
            elif col_type.startswith("subquery"):
                # TODO
                subquery_in_select_clauses = generate_subquery(
                    schema,
                    schema_types,
                    db_name,
                    attributes,
                    col_type,
                    pk,
                    fk,
                    min_max_depth_in_subquery=min_max_depth_in_subquery,
                    query_generator_single_schema_func=query_generator_single_schema_func,
                )

                queries = []
                for clause, alias_name, attributes in subquery_in_select_clauses:
                    # attributes = get_all_attributes_for_from_subquery()
                    # queries.append([query, attributes, must_have_attributes])
                    # TODO

                    select_statement = "SELECT " + clause
                    select_fields.append(alias_name)
                    num_value_exp = 1
                    select_fields_types[random_column] = "text"
            num_value_exps += num_value_exp  # Increment the number of value expressions
        if select_statement[-2:] == ", ":
            select_statement = select_statement[
                :-2
            ]  # Remove the trailing comma and space
        select_statements.append(
            [select_statement, select_fields, num_value_exps, select_fields_types]
        )
        # Add the generated select_statement and select_fields to select_statements
    return select_statements
