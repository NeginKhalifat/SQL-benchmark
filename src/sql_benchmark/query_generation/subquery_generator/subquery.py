import copy
import json
import os
import random

from helper_funcs import get_table_name_from_column, read_random_specs


def generate_subquery(
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables=None,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
    having=False,
):

    if min_max_depth_in_subquery is None:
        min_max_depth_in_subquery = [0, 0]
    current_dir = os.path.dirname(__file__)
    file_name = os.path.join(current_dir, f"../output/specs/{db_name}.json")

    if subquery_type in ["in_with_subquery", "not_in_with_subquery"]:
        return generate_in_or_not_in_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_single_schema_func,
            having=having,
        )

    elif subquery_type == "comparison_with_subquery":
        return generate_comparison_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_single_schema_func,
            having=having,
        )

    elif subquery_type in ["exists_subquery", "not_exists_subquery"]:
        return generate_exists_subquery(
            file_name,
            schema,
            schema_types,
            db_name,
            colms,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_single_schema_func,
            having=having,
        )

    elif subquery_type == "subquery":
        return generate_subquery_for_from_clause(
            file_name,
            schema,
            schema_types,
            db_name,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery,
            query_generator_single_schema_func,
        )

    elif subquery_type == "subquery_exp_alias":
        ("subquery_exp_alias!!!")
        return generate_subquery_exp_alias(
            file_name,
            schema,
            schema_types,
            db_name,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )
    elif subquery_type == "CTE":
        print("subquery_in_CTE^^")

        return generate_subquery_CTE(
            file_name,
            schema,
            schema_types,
            db_name,
            subquery_type,
            pk,
            fk,
            tables,
            min_max_depth_in_subquery=min_max_depth_in_subquery,
            query_generator_single_schema_func=query_generator_single_schema_func,
        )


def generate_in_or_not_in_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_single_schema_func,
    having=False,
):
    in_or_not_in = "NOT IN" if "not" in subquery_type else "IN"

    random_column = random.choice(colms["number"] + colms["text"]+colms["time"])
    if "." in random_column:

        # print(schema[random_column.split(".")[0]])
        random_column = random_column.split(".")[1]

    must_be_in_select = [random_column]

    tables = get_table_name_from_column(random_column, schema)
    if tables == []:
        for table in schema:
            if table.startswith("CTE"):
                tables = [table]
                break

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name, db_name, tables, pk, fk, min_max_depth_in_subquery, having=having
    )

    dict_spec = {db_name: {spec_hash: spec}}

    try:
        merged_queries = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            must_be_in_select=must_be_in_select,
            write_to_csv=False,
            is_subquery=True,
            random_choice=True,
        )

        sub_query = list(merged_queries.values())[0].split("\n")[0]

        where_clause = f"{random_column} {in_or_not_in} ({sub_query})"
        return [where_clause]
    except Exception as e:
        raise Exception(f"Error in IN/NOT IN subquery: {str(e)}")


def generate_comparison_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_single_schema_func,
    having=False,
):
    comparison_operators = ["=", "!=", ">", "<", ">=", "<="]
    comp_clause = random.choice(comparison_operators)

    random_column = random.choice(colms["number"] + colms["text"]+colms["time"])
    if "." in random_column:
        random_column = random_column.split(".")[1]
    tables = get_table_name_from_column(random_column, schema)

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name, db_name, tables, pk, fk, min_max_depth_in_subquery, having=having
    )
    agg_func = random.choice(["MAX", "MIN", "AVG", "SUM"])
    must_be_in_select = [f"{agg_func}({random_column})"]

    dict_spec = {db_name: {spec_hash: spec}}
    try:
        merged_queries = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            must_be_in_select=must_be_in_select,
            write_to_csv=False,
            is_subquery=True,
            random_choice=True,
        )

        sub_query = list(merged_queries.values())[0].split("\n")[0]

        where_clause = f"{random_column} {comp_clause} ({sub_query})"

        return [where_clause]
    except Exception as e:
        raise Exception(f"Error in comparison subquery: {e}")


def generate_exists_subquery(
    file_name,
    schema,
    schema_types,
    db_name,
    colms,
    subquery_type,
    pk,
    fk,
    tables,
    min_max_depth_in_subquery,
    query_generator_single_schema_func,
    having=False,
):
    print("generate_exists_subquery")
    exist_or_not_exist = "EXISTS" if "exists" in subquery_type else "NOT EXISTS"

    must_be_in_select = random.choice([["*"]])

    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name,
        db_name,
        tables,
        pk,
        fk,
        min_max_depth_in_subquery,
        schema=schema,
        exists=True,
        having=having,
        colms=colms,
        schema_types=schema_types,
    )
    dict_spec = {db_name: {spec_hash: spec}}
    try:
        print("query_generator_single_schema_func")
        print(must_be_in_select)
        print(must_be_in_where)
        merged_queries = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            must_be_in_select=must_be_in_select,
            must_be_in_where=must_be_in_where,
            write_to_csv=False,
            is_subquery=True,
            random_choice=True,
        )
        print("DONEEEE")
    except Exception as e:
        # print(e)
        raise Exception(f"Error in EXISTS/NOT EXISTS subquery: {e}")
    else:
        print(merged_queries)
        sub_query = list(merged_queries.values())[0].split("\n")[0]
        print(sub_query)
        print("&&&&&&&&&&&&&&&&&&&")

        where_clause = f"{exist_or_not_exist} ({sub_query})"

        return [where_clause]


def generate_subquery_for_from_clause(
    file_name,
    schema,
    schema_types,
    db_name,
    subquery_type,
    pk,
    fk,
    tables=None,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name, db_name, tables, pk, fk, min_max_depth_in_subquery, from_clause=True
    )
    dict_spec = {db_name: {spec_hash: spec}}
    try:
        merged_queries, select_fields = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            write_to_csv=False,
            is_subquery=False,
            random_choice=True,
            return_select_fields=True,
            return_table_exp_attributes=True,
            return_unique_tables=True,
            rename_must_be_in_select=True,
        )
        hash = list(select_fields.keys())[0]
        select_fields_list = select_fields[hash]["select_fields"]
        query_attrs = select_fields[hash]["table_exp_attributes"]
        unique_tables = select_fields[hash]["unique_tables"]
        select_fields_types = select_fields[hash]["select_fields_types"]
        (select_fields_types)
        attributes = {"number": [], "text": [], "time": []}
        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")

        for field in select_fields_list:
            if field in query_attrs["number"]:
                if "." in field:
                    field = field.split(".")[1]
                attributes["number"].append(f"{alias_name}.{field}")
            elif field in query_attrs["text"]:
                if "." in field:
                    field = field.split(".")[1]
                attributes["text"].append(f"{alias_name}.{field}")
            elif field in query_attrs["time"]:
                if "." in field:
                    field = field.split(".")[1]
                attributes["time"].append(f"{alias_name}.{field}")
            elif field in select_fields_types:
                if "." in field:
                    field = field.split(".")[1]
                attributes[select_fields_types[field]].append(f"{alias_name}.{field}")

        sub_query = list(merged_queries.values())[0].split("\n")[0]

        from_clause_subquery = f"({sub_query}) AS {alias_name}"

        return [
            [
                from_clause_subquery,
                {alias_name: unique_tables},
                attributes,
            ]
        ]
    except Exception as e:
        raise Exception(f"Error in subquery for FROM clause: {e}")


def generate_subquery_CTE(
    file_name,
    schema,
    schema_types,
    db_name,
    subquery_type,
    pk,
    fk,
    tables=None,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    print("generate_subquery_CTE")
    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name,
        db_name,
        tables,
        pk,
        fk,
        min_max_depth_in_subquery,
        subquery_in_select_statement=True,
        CTE=True,
    )

    dict_spec = {db_name: {spec_hash: spec}}

    try:
        merged_queries, select_fields = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            write_to_csv=False,
            is_subquery=False,
            random_choice=True,
            return_select_fields=True,
            return_table_exp_attributes=True,
            return_unique_tables=True,
        )
        hash = list(select_fields.keys())[0]
        select_fields_list = select_fields[hash]["select_fields"]
        query_attrs = select_fields[hash]["table_exp_attributes"]
        unique_tables = select_fields[hash]["unique_tables"]
        select_fields_types = select_fields[hash]["select_fields_types"]
        attributes = {"number": [], "text": []}
        alias_name = "CTE_" + random.choice("1234567890")

        for field in select_fields_list:
            if "." in field:
                field = field.split(".")[1]
            if field in query_attrs["number"]:
                attributes["number"].append(f"{alias_name}.{field}")
            elif field in query_attrs["text"]:
                attributes["text"].append(f"{alias_name}.{field}")
            elif field in query_attrs["time"]:
                attributes["time"].append(f"{alias_name}.{field}")
            elif field in select_fields_types:
                attributes[select_fields_types[field]].append(f"{alias_name}.{field}")

        sub_query = list(merged_queries.values())[0].split("\n")[0]

        cte_subquery = f"WITH {alias_name} AS ({sub_query})\n"

        return [
            [
                cte_subquery,
                {alias_name: unique_tables},
                attributes,
            ]
        ]
    except Exception as e:
        raise Exception(f"Error in subquery for CTE: {e}")


def generate_subquery_exp_alias(
    file_name,
    schema,
    schema_types,
    db_name,
    subquery_type,
    pk,
    fk,
    tables=None,
    min_max_depth_in_subquery=None,
    query_generator_single_schema_func=None,
):
    spec, spec_hash, must_be_in_where = read_random_specs(
        file_name,
        db_name,
        tables,
        pk,
        fk,
        min_max_depth_in_subquery,
        subquery_in_select_statement=True,
    )

    dict_spec = {db_name: {spec_hash: spec}}

    try:
        merged_queries, select_fields = query_generator_single_schema_func(
            schema=schema,
            schema_types=schema_types,
            pk=pk,
            fk=fk,
            specs=dict_spec,
            db_name=db_name,
            write_to_csv=False,
            is_subquery=False,
            random_choice=True,
            return_select_fields=True,
            return_table_exp_attributes=True,
            return_unique_tables=True,
        )
        hash = list(select_fields.keys())[0]
        select_fields_list = select_fields[hash]["select_fields"]
        query_attrs = select_fields[hash]["table_exp_attributes"]
        unique_tables = select_fields[hash]["unique_tables"]
        select_fields_types = select_fields[hash]["select_fields_types"]
        (select_fields_types)
        attributes = {"number": [], "text": [],"time": []}
        alias_name = random.choice("abcdefghijklmnopqrstuvwxyz")

        for field in select_fields_list:
            if field in query_attrs["number"]:
                attributes["number"].append(f"{alias_name}.{field}")
            elif field in query_attrs["text"]:
                attributes["text"].append(f"{alias_name}.{field}")
            elif field in query_attrs["time"]:
                attributes["time"].append(f"{alias_name}.{field}")
            elif field in select_fields_types:
                attributes[select_fields_types[field]].append(f"{alias_name}.{field}")

        sub_query = list(merged_queries.values())[0].split("\n")[0]
        from_clause_subquery = f"({sub_query}) AS {alias_name}"

        return [
            [
                from_clause_subquery,
                alias_name,
                attributes,
            ]
        ]
    except Exception as e:
        raise Exception(f"Error in subquery for exp_alias: {e}")
