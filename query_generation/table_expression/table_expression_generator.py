import sys

sys.path.append("..")


from subquery_generator import generate_subquery

from .table_expression_helper_funcs import (
    handle_join_case,
    handle_single_table_case,
    handle_single_table_with_name_changing_case,
    handle_table_expression_for_CTE,
    handle_table_expression_for_subquery,
)


def create_table_expression(
    schema,
    pk,
    fk,
    schema_types,
    table_expression_type,
    meaningful_joins,
    db_name,
    random_choice=False,
    query_generator_single_schema_func=None,
    min_max_depth_in_subquery=None,
    cte="",
):
    """
    Generate SQL table expression based on the specified type.

    Args:
        schema (dict): Database schema with table names and their columns.
        pk (dict): Primary keys for tables.
        fk (dict): Foreign keys for tables.
        schema_types (dict): Data types of columns in the schema.
        table_expression_type (str or dict): Type of table expression to generate.
        meaningful_joins (str): Type of join - meaningful or meaningless.
        random_choice (bool, optional): Indicates if random choice is enabled. Defaults to False.

    Returns:
        list: List of queries with attributes based on the specified table expression type.
    """
    print("table_expression_type", table_expression_type)
    case = table_expression_type
    queries_with_attributes = []

    if isinstance(
        case, dict
    ):  # case=={"single_table": "city"} mainly used for subquery
        queries_with_attributes = handle_table_expression_for_subquery(
            case, schema, schema_types, random_choice, cte
        )
    elif case == "single_table":  # case=="single_table"
        queries_with_attributes = handle_single_table_case(
            schema, schema_types, random_choice, cte
        )
    elif (
        case == "single_table_with_name_changing"
    ):  # case=="single_table_with_name_changing"
        queries_with_attributes = handle_single_table_with_name_changing_case(
            schema, schema_types, random_choice, cte
        )
    elif "JOIN" in case: 
        queries_with_attributes = handle_join_case(
            case, schema, fk, schema_types, meaningful_joins, pk, random_choice, CTE=cte
        )

    elif case == "CTE":
        cte = handle_table_expression_for_CTE(
            schema,
            schema_types,
            db_name,
            None,
            case,
            pk,
            fk,
            tables=None,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )

        alias_name_table = list(cte[1].keys())[0]
        attributes = cte[2]["number"] + cte[2]["text"]+ cte[2]["time"]
        schema[alias_name_table] = []
        schema_types[alias_name_table] = {}
        for attribute in attributes:
            for pk_table in pk:
                real_attribute = attribute.split(".")[1]
                if real_attribute in pk[pk_table]:
                    pk[alias_name_table] = attribute
                    fk[alias_name_table] = pk_table
                    break
            schema[alias_name_table].append(attribute)
            if attribute in cte[2]["time"]:
                schema_types[alias_name_table][attribute] = "time"
            elif attribute in cte[2]["text"]:
                schema_types[alias_name_table][attribute] = "text"
            else:
                schema_types[alias_name_table][attribute] = "number"
            
        print("DONEEEEEE CTE##")

        queries_with_attributes = create_table_expression(
            schema,
            pk,
            fk,
            schema_types,
            cte[1],
            meaningful_joins,
            db_name,
            random_choice=True,
            query_generator_single_schema_func=query_generator_single_schema_func,
            cte=cte,
        )

    elif case == "subquery":
        try:
            from_clauses = generate_subquery(
                schema,
                schema_types,
                db_name,
                None,
                case,
                pk,
                fk,
                min_max_depth_in_subquery=min_max_depth_in_subquery,
                query_generator_single_schema_func=query_generator_single_schema_func,
                having=True,
            )
            for from_clause, tables, attributes in from_clauses:
                query = f" FROM {from_clause}"
                # attributes = get_all_attributes_for_from_subquery()
                # queries.append([query, attributes, must_have_attributes])
                # TODO
                queries_with_attributes.append([query, tables, attributes, cte])
        except Exception as e:
            raise e
    return queries_with_attributes
