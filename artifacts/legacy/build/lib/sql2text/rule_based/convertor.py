import json
import re

import inflect
import pandas as pd

from query_generation.read_schema.read_schema import convert_json_to_schema

inflect = inflect.engine()

from parser_sql.parse_sql_one import Schema, get_schemas_from_json, get_sql

AGG_OPS = ("none", "max", "min", "count", "sum", "avg")
UNIT_OPS_LIST = ["none", "-", "+", "*", "/"]
UNIT_OPS = {
    "none": "",
    "-": "negation of",
    "+": "sum of",
    "*": "multiplication of",
    "/": "division of",
}
WHERE_OPS_LIST = (
    "not",
    "between",
    "=",
    ">",
    "<",
    ">=",
    "<=",
    "!=",
    "in",
    "like",
    "is",
    "exists",
)
WHERE_OPS = {
    "not": "not",
    "between": "between",
    "=": "equal to",
    ">": "greater than",
    "<": "less than",
    ">=": "greater than or equal to",
    "<=": "less than or equal to",
    "!=": "not equal to",
    "in": "in",
    "like": "like",
    "is": "is",
    "exists": "exists",
}


def remove_extra_dots(sentence):
    # Remove extra dots followed by whitespace
    cleaned_sentence = re.sub(r"\.\s*\.", ".", sentence)
    # Remove extra dots at the end of the sentence
    cleaned_sentence = re.sub(r"\.\s*$", "", cleaned_sentence)
    return cleaned_sentence


def extract_condition(
    sql_query, where_or_having, schema_info, table_file, db_id, join_rel_list
):
    if not sql_query[where_or_having]:
        return ""

    where_condition = "where "
    if where_or_having == "where":
        where_info = sql_query["where"]

    else:
        where_info = sql_query["having"]

    for cond_unit in where_info:
        if cond_unit == "and":
            where_condition += " and "
            continue
        elif cond_unit == "or":
            where_condition += " or "
            continue

        not_op, op_id, val_unit, val1, val2 = cond_unit
        if isinstance(val1, dict):
            val1 = convert_sql_to_text(
                schema_info, table_file, db_id, join_rel_list, pasrsed_sql=val1
            )

        not_op = "not" if not_op else ""

        where_op = (
            WHERE_OPS[WHERE_OPS_LIST[op_id]] if WHERE_OPS_LIST[op_id] != "none" else ""
        )
        val_exp = []

        extract_val_unit(val_unit, val_exp, schema_info)
        if isinstance(val1, tuple):
            val1_exp = []
            extract_col_unit(val1, val1_exp, schema_info)
            val1 = ", ".join(val1_exp)
        if val2 and isinstance(val2, tuple):
            val2_exp = []
            extract_col_unit(val2, val2_exp, schema_info)
            val2 = ", ".join(val2_exp)
        # Joining selected columns for the SELECT clause

        val_exp = ", ".join(val_exp)
        if val2 is not None:
            where_condition += f"the {val_exp} is {not_op} {where_op} {val1} and {val2}"
        else:
            where_condition += f"the {val_exp} is {not_op} {where_op} {val1}"
    where_condition = " ".join(where_condition.split())
    return where_condition


def extract_select_statement(sql_query, schema_info, table_file, db_id, join_rel_list):

    # Extracting select information
    select_info = sql_query["select"]
    is_distinct = "distinct" if select_info[0] else ""
    select_columns = []

    for agg_id, val_unit in select_info[1]:
        val_exp = []

        if isinstance(val_unit, dict):
            val_unit = convert_sql_to_text(
                schema_info, table_file, db_id, join_rel_list, pasrsed_sql=val_unit
            )
            val_exp.append("subquery in select: (" + val_unit + ")")

        else:
            extract_val_unit(val_unit, val_exp, schema_info)
        agg_func = AGG_OPS[agg_id] + " of" if AGG_OPS[agg_id] != "none" else ""

        val_exp = ", ".join(val_exp)
        if val_exp.split()[0] == "*" and not agg_func:
            val_exp = "all columns"
        select_columns.append(f"{agg_func} {val_exp}")

    select_columns_str = ", ".join(select_columns)
    select_columns_str = " ".join(select_columns_str.split())
    return is_distinct, select_columns_str


def extract_table_expression(sql_query, schema_info, table_file, db_id, join_rel):
    table_units = sql_query["from"]["table_units"]
    table_names = []
    for table_unit in table_units:
        if table_unit[0] == "sql":
            table_names.append(
                "this subquery:(select "
                + convert_sql_to_text(
                    schema_info, table_file, db_id, join_rel, pasrsed_sql=table_unit[1]
                )
                + ")"
            )
        else:
            table_id = table_unit[1]
            # print("iloc", schema_info["table_names_original"].iloc[0])
            table_name = schema_info["table_names_original"].iloc[0][table_id]
            if inflect.singular_noun(table_name) != None:
                plural_table_name = inflect.plural(table_name)
                table_names.append(plural_table_name)
            else:
                table_names.append(table_name)
    if len(table_units) == 2:
        for rel in join_rel:
            if rel[0] == inflect.singular_noun(table_names[0]) and rel[
                1
            ] == inflect.singular_noun(table_names[1]):
                table_names = table_names[0] + " that " + rel[2] + " " + table_names[1]
                break
    else:
        table_names = ", ".join(table_names)
    return table_names


def extract_col_unit(val_unit, col_units, schema_info):
    agg_id = val_unit[0]
    agg_func = AGG_OPS[agg_id] + " of " if AGG_OPS[agg_id] != "none" else ""
    col_id = val_unit[1]
    isDistinct = "distinct" if val_unit[2] else ""
    col_name = schema_info["column_names_original"].iloc[0][col_id][1]
    if (
        schema_info["column_types"].iloc[0][col_id] == "number"
        and col_id not in schema_info["primary_keys"]
    ):
        if inflect.singular_noun(col_name) != None:
            plural_col_name = inflect.plural(col_name)
            col_name = "number of " + plural_col_name
        else:
            col_name = "number of " + col_name
    col_name = f"{agg_func} {isDistinct} {col_name}"
    col_units.append(col_name)


def extract_val_unit(val_unit, val_exp, schema_info):
    unit_op = (
        UNIT_OPS[UNIT_OPS_LIST[val_unit[0]]]
        if UNIT_OPS_LIST[val_unit[0]] != "none"
        else ""
    )

    col_unit1 = val_unit[1]
    col_unit2 = val_unit[2]
    agg_id_1 = col_unit1[0]
    agg_func1 = AGG_OPS[agg_id_1] + " of " if AGG_OPS[agg_id_1] != "none" else ""
    col_id_1 = col_unit1[1]
    isDistinct_1 = "distinct" if col_unit1[2] else ""

    if col_unit2 is not None:
        agg_id_2 = col_unit2[0]
        agg_func2 = AGG_OPS[agg_id_2] + " of " if AGG_OPS[agg_id_2] != "none" else ""

        col_id_2 = col_unit2[1]
        isDistinct_2 = "distinct" if col_unit2[2] else ""
    col_name1 = schema_info["column_names_original"].iloc[0][col_id_1][1]

    if (
        schema_info["column_types"].iloc[0][col_id_1] == "number"
        and col_id_1 not in schema_info["primary_keys"]
    ):
        if inflect.singular_noun(col_name1) != None:
            plural_col_name1 = inflect.plural(col_name1)
            col_name1 = "number of " + plural_col_name1
        else:
            col_name1 = "number of " + col_name1

    col_name1 = f" {agg_func1} {isDistinct_1} {col_name1}"

    if col_unit2 is not None:

        col_name2 = schema_info["column_names_original"].iloc[0][col_id_2][1]
        if (
            schema_info["column_types"].iloc[0][col_id_2] == "number"
            and col_id_2 not in schema_info["primary_keys"]
        ):
            if inflect.singular_noun(col_name2) != None:
                plural_col_name2 = inflect.plural(col_name2)
                col_name2 = "number of " + plural_col_name2
            else:
                col_name2 = "number of " + col_name2

        col_name2 = f"{agg_func2} {isDistinct_2} {col_name2}"
    if unit_op:
        val_exp.append(f"{unit_op} {col_name1} and {col_name2}")
    else:
        val_exp.append(f" {col_name1}")


def extract_group_by_clause(sql_query, schema_info):
    group_by_clause = ""
    if sql_query["groupBy"]:

        group_by_clause = "for each "
        group_by_columns = []
        for val_unit in sql_query["groupBy"]:
            col_units = []
            extract_col_unit(val_unit, col_units, schema_info)

        col_units = " and, ".join(col_units)
        col_units = " ".join(col_units.split())
        group_by_columns.append(col_units)
        group_by_clause += ", ".join(group_by_columns)
    return group_by_clause


def extract_order_by_clause(sql_query, schema_info):
    if sql_query["orderBy"]:
        order_by_clause = "The result should be ordered by "
        order_by_columns = []
        val_exp = []
        is_asc = True if sql_query["orderBy"][0] == "asc" else False
        for val_unit in sql_query["orderBy"][1]:
            extract_val_unit(val_unit, val_exp, schema_info)
        val_exp = " and, ".join(val_exp)
        val_exp = " ".join(val_exp.split())

        order_by_columns.append(
            f"{val_exp} in ascending order"
            if is_asc
            else f"{val_exp} in descending order"
        )
        order_by_clause += ", ".join(order_by_columns)
    else:
        order_by_clause = "The result can be in any order"
    return order_by_clause


def extract_limit_clause(sql_query, schema_info):
    limit_clause = ""
    if sql_query["limit"]:
        limit_clause = f"Return the first {sql_query['limit']} rows"

    return limit_clause


def extract_except_clause(sql_query, schema_info, table_file, db_id, join_rel_list):
    except_clause = ""
    if sql_query["except"]:
        except_clause = "except "
        except_query = convert_sql_to_text(
            schema_info,
            table_file,
            db_id,
            join_rel_list,
            pasrsed_sql=sql_query["except"],
        )
        except_clause += except_query
    return except_clause


def convert_sql_to_text(
    schema_info, table_file, db_id, join_rel, sql=None, pasrsed_sql=None
):
    # if len(sql[0]) > 1:
    #     sql = sql[0]
    # print(schema_info)
    schemas, db_names, tables = get_schemas_from_json(table_file)
    # print(db_names)
    # print(tables)‰
    schema = schemas[db_id]
    table = tables[db_id]
    schema = Schema(schema, table)
    if pasrsed_sql:
        sql_query = pasrsed_sql
    else:

        sql_query = get_sql(schema, sql)
    # print(sql_query)
    # print("_______________________")

    # Extracting select statement
    is_distinct, select_columns_str = extract_select_statement(
        sql_query, schema_info, table_file, db_id, join_rel
    )
    # print("Select columns", select_columns_str)
    # Extracting table expression
    table_exp = extract_table_expression(
        sql_query, schema_info, table_file, db_id, join_rel
    )
    # print("Table expression", table_exp)
    # Extracting where condition
    where_condition = extract_condition(
        sql_query, "where", schema_info, table_file, db_id, join_rel_list=join_rel
    )
    # print("Where condition", where_condition)
    # Extracting order by clause
    group_by_clause = extract_group_by_clause(sql_query, schema_info)
    # print("Group by clause", group_by_clause)
    having_clause = extract_condition(
        sql_query,
        "having",
        schema_info,
        table_file=table_file,
        db_id=db_id,
        join_rel_list=join_rel,
    )
    # print("Having clause", having_clause)
    order_by_clause = extract_order_by_clause(sql_query, schema_info)
    # print("Order by clause", order_by_clause)
    # Extracting limit clause
    limit_clause = extract_limit_clause(sql_query, schema_info)
    # print("Limit clause", limit_clause)
    # Generating natural language text
    except_clause = extract_except_clause(
        sql_query,
        schema_info,
        table_file=table_file,
        db_id=db_id,
        join_rel_list=join_rel,
    )
    # print("Except clause", except_clause)
    if pasrsed_sql:
        text = f"{is_distinct} {select_columns_str} {group_by_clause} {having_clause} from {table_exp} {where_condition}.\n {limit_clause}. {order_by_clause} ."
    else:
        text = f"Retrieve {is_distinct} {select_columns_str} {group_by_clause} {having_clause} from {table_exp} {where_condition} {except_clause}.\n{limit_clause}. {order_by_clause} ."
    text = " ".join(text.split())
    text = remove_extra_dots(text)
    return text.strip()


def convert_to_triples(string):
    # Remove leading and trailing whitespace, and split the string by lines
    lines = string.strip().split("),")

    # Initialize an empty list to store triples
    triples = []
    j = 0
    # Iterate over each line
    for line in lines:

        # Remove leading and trailing whitespace, and split the line by comma and space
        parts = line.strip().split(", ")
        if j == 0:
            triples.append((parts[0][3:-1], parts[1][1:-1], parts[2][1:-1]))
        elif j == len(lines) - 1:
            triples.append((parts[0][2:-1], parts[1][1:-1], parts[2][1:-3]))

        else:
            # Extract the three elements and append them as a tuple to the triples list
            triples.append((parts[0][2:-1], parts[1][1:-1], parts[2][1:-1]))
        j += 1

    return triples


# Example usage
if __name__ == "__main__":
    all = convert_json_to_schema("data/tables.json")

    join_rel_dict = {}
    wrong_rel_db = []
    with open("data/join-relationship/table_relations.json", "r") as f:
        # Read each line from the file

        for line in f:
            # Parse the JSON object from the line
            table_relation = json.loads(line)
            # print(list(table_relation.keys())[0])

            if isinstance(list(table_relation.values())[0][0], dict):
                # Convert the second dictionary to the format of the first dictionary
                table_relation = {
                    key: [[d["entity1"], d["entity2"], d["verb"]] for d in value]
                    for key, value in table_relation.items()
                }

            if list(table_relation.values())[0][0][0] not in list(
                all[list(table_relation.keys())[0]]["schema"].keys()
            ):
                wrong_rel_db.append(list(table_relation.keys())[0])
            else:
                join_rel_dict.update(table_relation)

    all_schema = pd.read_json("data/tables.json")
    
    db_name = "farm"
    schema_info = all_schema.query(f"db_id == '{db_name}'")
   
    sql = """SELECT Farm_ID AS Farm_ID_w, Competition_ID, Rank FROM competition_record WHERE  Competition_ID >= Competition_ID GROUP BY Rank ORDER BY Rank ASC"""
    table_file = "data/tables.json"
    
    all_train_ds = pd.read_json("data/train_spider.json")


    print(
        convert_sql_to_text(
            schema_info, table_file, db_name, join_rel_dict[db_name], sql=sql
        )
    )
   