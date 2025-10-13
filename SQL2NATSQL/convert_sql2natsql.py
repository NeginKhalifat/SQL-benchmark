from query_generation.read_schema.read_schema import convert_json_to_schema
from parser_sql.parse_sql_one import (
    get_schemas_from_json,
    Schema,
    get_tables_with_alias,
    tokenize,
    get_sql,
)
import re


def handle_select_clause(select_exp, schema_info, tables_with_alias, tables):
    # Split the select_clause into individual columns
    select_clause = []
    columns = select_exp.split(",")

    def process_table_column(value_exp, schema_info):
        if "." in value_exp:
            table_name, column_name = value_exp.split(".")
            if column_name == "*":
                # Get all columns of the table
                if table_name in schema_info:
                    table_col = f"{table_name}.*"
                else:
                    table_col = f"{tables_with_alias[table_name.lower()]}.*"

            else:
                # Check if the column exists in the table

                table_col = f"{tables_with_alias[table_name.lower()]}.{column_name}"

        else:
            if "*" in value_exp:
                # Get all columns of the table
                table_name = tables[0]
                table_col = f"{table_name}.*"
            else:
                table_col = None
                for table in schema_info["schema"]:
                    if value_exp in schema_info["schema"][table]:
                        table_col = f"{table}.{value_exp}"
        return table_col

    def process_column(column, select_clause):
        # Check if it is an aggregate column
        if "(" in column and ")" in column:
            # Extract the aggregate function and table column
            if len(column.split("(")) == 2:
                aggregate_function, table_col = column.split("(")

                table_col = process_table_column(table_col, schema_info)
                select_clause.append(f"{aggregate_function} ( {table_col} )")
        else:
            # Process the table column
            table_col = process_table_column(column, schema_info)
        return table_col

    table_cols = []
    for column in columns:
        # Process each column
        table_cols.append(process_column(column.strip(), select_clause))
    return table_cols


def find_columns_to_replace(t_list, table_r, schema):
    def has_foreign_key_relationship(table, table_r):
        print("table", table)
        print("table_r", table_r)
        # Check if there is a foreign key relationship between table and table_r
        if table in schema["foreign_keys"]:
            key_values = list(schema["foreign_keys"][table].values())
            for table_, key in key_values:
                if table_r == table_:

                    return (
                        True,
                        key,
                        list(schema["foreign_keys"][table].keys())[0],
                        table_r,
                        table,
                    )

        elif table_r in schema["foreign_keys"]:
            key_values = list(schema["foreign_keys"][table_r].values())
            for table_, key in key_values:
                if table == table_:
                    return (
                        True,
                        key,
                        list(schema["foreign_keys"][table_r].keys())[0],
                        table,
                        table_r,
                    )

        return False, None, None, None, None

    def get_foreign_key_columns(table, table_r, key1, key2):
        # Get the foreign key columns between table and table_r
        return schema["foreign_keys"][table][table_r]

    def has_same_name_columns(table, table_r):
        # Check if there are columns with the same name in both table and table_r
        if table in schema["schema"] and table_r in schema["schema"]:
            common_columns = set(schema["schema"][table]).intersection(
                schema["schema"][table_r]
            )
            if common_columns:
                return True
        return False

    def get_same_name_columns(table, table_r):
        # Get the columns with the same name in both table and table_r
        common_columns = set(schema["schema"][table]).intersection(
            schema["schema"][table_r]
        )
        return list(common_columns)

    def get_primary_keys(table):
        # Get the primary key columns of the table
        if table in schema["primary_keys"]:
            return schema["primary_keys"][table]
        return []

    for table in t_list:
        # Check if there is a foreign key relationship between table and table_r
        output, key1, key2, t1, t2 = has_foreign_key_relationship(table, table_r)
        if output:
            return key1, key2, t1, t2

    # for table in t_list:
    #     # Check if there are columns with the same name in both table and table_r
    #     if has_same_name_columns(table, table_r):
    #         return get_same_name_columns(table, table_r)

    return get_primary_keys(table_r)


def handle_where_clause(conditions, tables, schema_info):
    w_opers = [
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
        "not in",
        "not like",
        "not between",
        "is not",
        "join",
    ]
    where_cond = ""
    conditions, conjunct_matches = conditions
    print("conditions@@", conditions)
    for cond in conditions:
        key_r_idx = 0
        print("cond", cond)
        # Check if the condition is a subquery
        if "SELECT" in cond:
            # Recursively convert the subquery to natural language
            tokenized_subquery = tokenize(cond)
            print("tokenized_subquery", tokenized_subquery)

            t_list = tables
            table_r = tokenized_subquery[tokenized_subquery.index("from") + 1]
            print("table_r", table_r)
            print("t_list", t_list)
            key1, key2, t1, t2 = find_columns_to_replace(t_list, table_r, schema_info)
            print("key1", key1)
            print("key2", key2)
            par_idx = tokenized_subquery.index("(")
            i = 0
            while i < len(tokenized_subquery):
                print("tokenized", tokenized_subquery[i])
                if not tokenized_subquery[i]:
                    continue
                elif tokenized_subquery[i] == ")":
                    break
                print("KKKKK", key_r_idx, i)
                print(tokenized_subquery[i])
                if tokenized_subquery[i] == tokenize(key1)[0]:
                    tokenized_subquery[i] = "@.@"
                elif i == par_idx:
                    # remove value from tokenized_subquery
                    tokenized_subquery[i] = ""
                    tokenized_subquery[i + 1] = ""
                    i += 1
                elif i > par_idx:
                    print("HUHUHU")
                    print("tok", tokenized_subquery[i])
                    if tokenized_subquery[i] == tokenize(key2)[0]:
                        key_r_idx = i
                        print("HEER")

                        tokenized_subquery[i] = f"{t2}.*"
                        print(key_r_idx, i)
                    elif i > key_r_idx:
                        print("GH")
                        try:
                            min_index = min(
                                [
                                    tokenized_subquery.index(clause)
                                    for clause in [
                                        "where",
                                        "group by",
                                        "having",
                                        "order by",
                                        "limit",
                                    ]
                                    if clause in tokenized_subquery
                                    and tokenized_subquery.index(clause) > i
                                ]
                            )
                            print("MIN INDEX", min_index)
                            i = min_index

                            # where_cond += " ".join(tokenized_subquery[i:min_index])
                        except:
                            pass

                where_cond += tokenized_subquery[i] + " "
                i += 1
                print("where_cond", where_cond)

                # if tokenized_subquery[i] == key2:
            # remove empty strings
            where_cond = [word for word in where_cond.split(" ") if word]

            where_cond = " ".join(where_cond)
            print("where_cond", where_cond)

        else:

            return


def sql2nat(sql_query, schema_info, tables_with_alias):
    tables = get_tables_in_from(
        get_sql(schema, sql_query)["from"]["table_units"], schema.schema
    )

    # Define the clauses to look for
    clauses = ["WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]
    select_index = sql_query.find("SELECT")
    if select_index != -1:
        clauses.append("FROM")
        min_index = min(
            sql_query.find(clause) for clause in clauses if clause in sql_query
        )
        select_clause = sql_query[select_index + 6 : min_index]
        from_index = sql_query.find("FROM")
        clauses.remove("FROM")
        print("select_clause", select_clause)

        table_cols = handle_select_clause(
            select_clause,
            schema_info=schema_info,
            tables_with_alias=tables_with_alias,
            tables=tables,
        )
        table_cols = ", ".join(table_cols)
    where_index = sql_query.find("WHERE")
    if where_index != -1:
        clauses.remove("WHERE")
        try:
            min_index = min(
                sql_query.find(clause) for clause in clauses if clause in sql_query
            )
            where_clause = sql_query[where_index + 5 : min_index]
        except:
            where_clause = sql_query[where_index + 5 :]
        clauses.append("WHERE")
    conditons = split_where_clause(where_clause)
    conditions = handle_where_clause(conditons, tables, schema_info)
    print("conditions", conditons)
    # Initialize the result string
    from_index = sql_query.find("FROM")
    if from_index != -1:
        min_index = min(
            sql_query.find(clause)
            for clause in clauses
            if clause in sql_query and sql_query.find(clause) > where_index
        )
        sql_query = "SELECT " + table_cols + " " + sql_query[min_index:]

    # remove group by
    group_by_index = sql_query.find("GROUP BY")
    if group_by_index != -1:
        min_index = min(
            sql_query.find(clause)
            for clause in clauses
            if clause in sql_query and sql_query.find(clause) > group_by_index
        )
        sql_query = sql_query[:group_by_index] + sql_query[min_index:]
    # merges the HAVING and WHERE
    having_index = sql_query.find("HAVING")
    if having_index != -1:
        min_index = min(
            sql_query.find(clause)
            for clause in clauses
            if clause in sql_query and sql_query.find(clause) > having_index
        )
        having_clause = sql_query[having_index + 7 : min_index]
        sql_query = sql_query[:having_index] + sql_query[min_index:]
        print(sql_query)
        where_index = sql_query.find("WHERE")
        if where_index != -1:
            min_index = min(
                sql_query.find(clause)
                for clause in clauses
                if clause in sql_query and sql_query.find(clause) > where_index
            )
            print(min_index)
            sql_query = (
                sql_query[:min_index] + " AND " + having_clause + sql_query[min_index:]
            )
            # add the HAVING clause to the WHERE clause
            # sql_query = sql_query[:where_index] + sql_query[where_index:min_index]
        else:
            # remove the HAVING clause
            sql_query = sql_query[:having_index] + sql_query[min_index:]
    return sql_query


import re


def split_where_clause(where_clause):
    conjuncts = ["and", "or", "except", "intersect", "union", "sub"]
    # Create a regular expression pattern to match any of the conjunctions
    conjunct_pattern = "|".join(re.escape(c) for c in conjuncts)
    # Find all matches of the conjunctions in the WHERE clause
    conjunct_matches = re.findall(
        f"\\b(?:{conjunct_pattern})\\b", where_clause, flags=re.IGNORECASE
    )
    # Split the WHERE clause based on the conjunctions
    conditions = re.split(
        f"\\b(?:{conjunct_pattern})\\b", where_clause, flags=re.IGNORECASE
    )
    # Remove any leading or trailing whitespace from each condition
    conditions = [condition.strip() for condition in conditions]
    return conditions, conjunct_matches


def get_tables_in_from(table_units, schema):
    # Get the tables in the JOIN clause
    tables = []
    for table_unit, idx in table_units:
        tables.append(list(schema.keys())[idx])
    return tables


# Example SQL query
sql_query = """SELECT Official_Name FROM city WHERE City_ID NOT IN (SELECT Host_city_ID FROM farm_competition WHERE Year>22) and City_ID = 1"""

db_name = "farm"
all = convert_json_to_schema("data/tables.json")


table_file = "data/tables.json"

schemas, db_names, tables = get_schemas_from_json(table_file)
schema = schemas[db_name]
table = tables[db_name]
schema = Schema(schema, table)
toks = tokenize(sql_query)


tables_with_alias = get_tables_with_alias(schema.schema, toks)
print(all[db_name]["schema"])
# t = get_sql(schema, sql_query)["where"]


cleaned_sql_query = sql2nat(sql_query, all[db_name], tables_with_alias)
print("Cleaned SQL Query:")
print(cleaned_sql_query)
