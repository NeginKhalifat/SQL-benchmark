import random

from helper_funcs import generate_arithmetic_expression, generate_random_alias_name


def handle_single_exp(select_statement, select_fields, number_or_text_or_time, attributes):
    """
    Handles the 'single_expl' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        random_column (str): The random column.

    Returns:
        tuple: The updated select statement and select fields.
    """
    if len(attributes[number_or_text_or_time]) == 0:
        raise Exception(f"No {number_or_text_or_time} columns in single_exp")
    if number_or_text_or_time == "number":
        possible_columns = []
        for i in attributes["number"]:
            # if "id" not in i.lower():
            possible_columns.append(i)
        if len(possible_columns) == 0:
            raise Exception("No number columns in single_exp")
        random_column = random.choice(possible_columns)
    else:
        random_column = random.choice(attributes[number_or_text_or_time])
    select_statement += f"{random_column}, "
    select_fields.append(random_column)
    return select_statement, select_fields


def handle_arithmatic_exp(
    select_statement,
    select_fields,
    col_type,
    random_column,
    attributes,
    select_fields_types,
):
    """
    Handles the 'arithmatic_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    arithmatic_exp = generate_arithmetic_expression(attributes, num_parts=2)
    if col_type == "arithmatic_exp":
        select_statement += f"{arithmatic_exp}, "
    else:
        alias_name = generate_random_alias_name(select_fields, "arithmatic_exp")
        select_statement += f"{arithmatic_exp} AS {alias_name}, "
        select_fields.append(alias_name)
        select_fields_types[alias_name] = "number"
    return select_statement, select_fields, 1


def handle_string_func_exp(
    select_statement,
    select_fields,
    col_type,
    random_column,
    attributes,
    select_fields_types,
):
    """
    Handles the 'string_func_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    random_string_func = random.choice(_STRING_FUNCTIONS)
    if len(attributes["text"]) == 0:
        raise Exception("No text columns in string_func_exp")
    random_column = random.choice(attributes["text"])

    if random_string_func == "SUBSTRING":
        select_statement += f"{random_string_func}({random_column}, 1, 3) "
    elif random_string_func == "REPLACE":
        select_statement += f"{random_string_func}({random_column}, 'a', 'b') "
    elif random_string_func == "CHARINDEX":
        select_statement += f"{random_string_func}('a', {random_column}) "
    elif random_string_func == "CONCAT":
        select_statement += f"{random_string_func}({random_column}, 'a') "
        # elif random_string_func in ["RIGHT", "LEFT"]:
        #     select_statement += f"{random_string_func}({random_column}, 3) "
    else:
        select_statement += f"{random_string_func}({random_column}) "

    if col_type == "string_func_exp_alias":
        alias_name = generate_random_alias_name(select_fields, "string_exp")
        select_statement += f"AS {alias_name}, "
        select_fields.append(alias_name)
        select_fields_types[alias_name] = "text"

    return select_statement, select_fields, 1


def handle_agg_exp(
    select_statement,
    select_fields,
    col_type,
    random_column,
    attributes,
    select_fields_types,
):
    """
    Handles the 'agg_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    print("HIIII")
    print(attributes)
    random_agg_func = random.choice(_AGGREGATE_FUNCTIONS)
    possible_columns = []
    # if len(attributes["number"]) == 1:
    #     possible_columns = attributes["number"]
    for i in attributes["number"]:
        # if "id" not in i.lower():
        possible_columns.append(i)
    if len(possible_columns) == 0:
        raise Exception("No number columns in agg_exp")
    print(possible_columns)
    random_column = random.choice(possible_columns)
    print(random_column)

    # select_fields.append(f"{random_agg_func}({random_column})")
    if col_type != "agg_exp":
        alias_name = generate_random_alias_name(select_fields, "agg_exp")
        select_statement += f"{random_agg_func}({random_column}) AS {alias_name}, "
        select_fields.append(alias_name)
        select_fields_types[alias_name] = "number"
    else:
        select_statement += f"{random_agg_func}({random_column}), "

    return select_statement, select_fields, 1


def handle_count_distinct_exp(
    select_statement,
    select_fields,
    col_type,
    random_column,
    attributes,
    select_fields_types,
):
    """
    Handles the 'count_distinct_exp' column type.

    Args:
        select_statement (str): The select statement.
        select_fields (list): The list of select fields.
        col_type (str): The column type.
        random_column (str): The random column.
        attributes (dict): The attributes dictionary.

    Returns:
        tuple: The updated select statement and select fields.
    """
    possible_columns = []
    # if len(attributes["number"]) == 1:
    #     possible_columns = attributes["number"]
    for i in attributes["number"]:
        # if "id" not in i.lower():
        possible_columns.append(i)
    possible_columns += attributes["text"]
    possible_columns += attributes["time"]
    if len(possible_columns) == 0:
        raise Exception("No number, text or time columns in count_distinct_exp")
    random_column = random.choice(possible_columns)
    # select_fields.append(f"COUNT(DISTINCT({random_column}))")

    if col_type != "count_distinct_exp":
        alias_name = generate_random_alias_name(select_fields, "count_exp")
        select_statement += f"COUNT(DISTINCT {random_column}) AS {alias_name}, "
        select_fields.append(alias_name)
        select_fields_types[alias_name] = "number"
    else:
        select_statement += f"COUNT(DISTINCT {random_column}), "
    return select_statement, select_fields, 1


_STRING_FUNCTIONS = [
    "UPPER",
    "LOWER",
    "LENGTH",
    # "CONCAT",
    "SUBSTRING",
    "REPLACE",
    # "TRIM",
    # "LEFT",
    # "RIGHT",
    "CHARINDEX",
]

_AGGREGATE_FUNCTIONS = ["MAX", "MIN", "AVG", "SUM", "COUNT"]
