import random

from subquery_generator import generate_subquery


def complete_with_having_clause(
    temp_query,
    attributes,
    must_have_attributes,
    having_clauses_types,
    schema,
    schema_types,
    db_name,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    random_choice=False,
):
    """
    Complete the query with the HAVING clause based on the given parameters.

    Args:
        temp_query (str): The temporary query.
        attributes (dict): A dictionary containing the attributes.
        must_have_attributes (list): The list of attributes that must be included.
        having_clauses_types (str or dict): The type(s) of HAVING clause to generate.
        random_choice (bool, optional): Whether to use random choice for certain parameters. Defaults to False.

    Returns:
        list: A list of completed queries with the HAVING clause.

    Examples:
        >>> complete_with_having_clause("SELECT * FROM table GROUP BY col2", {"number": ["col1"], "text": ["col2"]}, ["col1"], "multiple")
        [['SELECT * FROM table GROUP BY col2 HAVING ((MAX(col1) > 50) AND (MIN(col1) < 30))', {'number': ['col1'], 'text': ['col2']}, ['col1']]]
    """
    if having_clauses_types == "none":
        return [[temp_query, attributes, must_have_attributes]]
    elif "subquery" in having_clauses_types:
        having_clauses = generate_subquery(
            schema,
            schema_types,
            db_name,
            attributes,
            having_clauses_types,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
            having=True,
        )
        queries = []
        for having_clause in having_clauses:
            query = f"{temp_query} HAVING {having_clause} "
            queries.append([query, attributes, must_have_attributes])
        return queries

    elif having_clauses_types == "multiple":
        return create_multiple_having_clause(
            attributes, temp_query, must_have_attributes
        )
    if having_clauses_types["single"]:
        try:
            having_clauses = create_having_clause(
                attributes, having_clauses_types["single"], random_choice=random_choice
            )
        except Exception as e:
            print(e)
            raise e
        return [
            [
                f"{temp_query} HAVING {having_clause}",
                attributes,
                must_have_attributes,
            ]
            for having_clause in having_clauses
        ]


def create_multiple_having_clause(attributes, temp_query, must_have_attributes):
    """
    Create multiple HAVING clauses based on the given attributes.

    Args:
        attributes (dict): A dictionary containing the attributes.
        temp_query (str): The temporary query.
        must_have_attributes (list): The list of attributes that must be included.

    Returns:
        list: A list of completed queries with the HAVING clause.

    Examples:
        >>> create_multiple_having_clause({"number": ["col1"], "text": ["col2"]}, "SELECT * FROM table", ["col1"])
        [['SELECT * FROM table HAVING ((MAX(col1) > 50) AND (MIN(col1) < 30))', {'number': ['col1'], 'text': ['col2']}, ['col1']]]

    Notes:
        - At most, two HAVING conditions can be generated.

    """
    logical_ops = ["AND", "OR"]
    aggregate_functions = ["MAX", "MIN", "AVG", "SUM", "COUNT", "COUNT DISTINCT"]

    random_logical_op = random.choice(logical_ops)
    random_agg_func = random.sample(aggregate_functions, 2)
    try:
        having_clause1 = create_having_clause(
            attributes, random_agg_func[0], random_choice=True
        )

        having_clause2 = create_having_clause(
            attributes, random_agg_func[1], random_choice=True
        )
    except Exception as e:
        raise e
    return [
        [
            f"{temp_query} HAVING (({having_clause1[0]}) {random_logical_op} ({having_clause2[0]}))",
            attributes,
            must_have_attributes,
        ]
    ]


def create_having_clause(attributes, aggregate_function, random_choice=False):
    """
    Create the HAVING clause based on the given aggregate function and attributes.

    Args:
        attributes (dict): A dictionary containing the attributes.
        aggregate_function (str): The aggregate function to use in the HAVING clause.
        random_choice (bool, optional): Whether to use random choice for certain parameters. Defaults to False.

    Returns:
        list: A list of HAVING clauses.

    Examples:
        >>> create_having_clause({"number": ["col1"], "text": ["col2"]}, "COUNT DISTINCT", random_choice=True)
        ['COUNT(DISTINCT(col1)) > 50']
        >>> create_having_clause({"number": ["col1"], "text": ["col2"]}, "MAX")
        ['MAX(col1) > 50', 'MAX(col1) < 30']
    """
    ops = ["=", ">", "<", ">=", "<="]
    having_clauses = []
    if random_choice:
        ops = [random.choice(ops)]
    for op in ops:
        if aggregate_function == "COUNT DISTINCT":
            random_column = random.choice(attributes["number"] + attributes["text"]+ attributes["time"])
            having_clause = (
                f"COUNT(DISTINCT {random_column}) {op} {random.randint(1, 100)}"
            )
        else:
            if len(attributes["number"]) == 0:
                raise Exception("No number columns found")
            random_column = random.choice(attributes["number"])
            having_clause = (
                f"{aggregate_function}({random_column}) {op} {random.randint(1, 100)}"
            )
        having_clauses.append(having_clause)
    return having_clauses
