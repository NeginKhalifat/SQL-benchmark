import contextlib
import json
import os


def update_list_of_lists(lst, c):
    """
    Update a list of lists by incrementing the count of a specific element if it exists, or adding a new element with a count of 1.

    Args:
        lst (list): The list of lists to update.
        c: The element to update or add.

    Returns:
        None
    """
    for sub_list in lst:
        if sub_list[0] == c:
            sub_list[1] += 1
            return
    lst.append([c, 1])


def convert_json_to_schema(file_name,col_exp=False):
    """
    Convert JSON data to a database schema.

    Args:
        file_name (str): The name of the JSON file containing the data.

    Returns:
        dict: A dictionary representing the database schema.
    """
    with open(file_name, "r") as f:
        json_data = json.load(f)
        all_db = {}
        for db in json_data:
            all_db[db["db_id"]] = {}
            schema = construct_schema(db)
            primary_keys = construct_primary_keys(db)
            foreign_keys = construct_foreign_keys(db)
            schema_types = construct_schema_types(db)
            if col_exp:
                schema_desc = construct_schema(db, col_exp=True)
            all_db[db["db_id"]]["schema"] = schema
            all_db[db["db_id"]]["primary_keys"] = primary_keys
            all_db[db["db_id"]]["foreign_keys"] = foreign_keys
            all_db[db["db_id"]]["schema_types"] = schema_types
            if col_exp:
                all_db[db["db_id"]]["schema_desc"] = schema_desc    
        return all_db


def construct_schema(db,col_exp=False):
    """
    Construct the schema dictionary from the given database.

    Args:
        db (dict): The database dictionary.

    Returns:
        dict: The schema dictionary representing the tables and their columns.
    """
    if col_exp:
        return {
            table_name:{ 
                "table_desc": db["table_names"][index] ,
                "columns": 
                [
                [column[1],db["column_names"][i][1]] for i, column in enumerate( db["column_names_original"]) if column[0] == index
            ]}
            for index, table_name in enumerate(db["table_names_original"])
        }
    return {
        table_name: [
            column[1] for column in db["column_names_original"] if column[0] == index
        ]
        for index, table_name in enumerate(db["table_names_original"])
    }


def construct_primary_keys(db):
    """
    Construct the primary keys dictionary from the given database.

    Args:
        db (dict): The database dictionary.

    Returns:
        dict: The primary keys dictionary representing the tables and their primary key columns.
    """
    primary_keys = {}
    for index, table_name in enumerate(db["table_names_original"]):
        with contextlib.suppress(Exception):
            primary_keys[table_name] = db["column_names_original"][
                db["primary_keys"][index]
            ][1]
    return primary_keys


def construct_foreign_keys(db):
    """
    Construct the foreign keys dictionary from the given database.

    Args:
        db (dict): The database dictionary.

    Returns:
        dict: The foreign keys dictionary representing the tables and their foreign key relationships.
    """
    foreign_keys = {}
    if db["foreign_keys"]:
        counting_tables = []
        pairs = []

        for foreign_key in db["foreign_keys"]:
            local_column_index = foreign_key[0]
            local_column_index2 = foreign_key[1]

            table1 = db["table_names_original"][
                db["column_names_original"][local_column_index][0]
            ]
            column1 = db["column_names_original"][local_column_index][1]
            table2 = db["table_names_original"][
                db["column_names_original"][local_column_index2][0]
            ]
            column2 = db["column_names_original"][local_column_index2][1]

            update_list_of_lists(counting_tables, table1)
            update_list_of_lists(counting_tables, table2)
            pairs.append((table1, column1, table2, column2))

        sorted_counting_tables = sorted(
            counting_tables, key=lambda x: x[1], reverse=True
        )

        for table in sorted_counting_tables:
            flag = False
            foreign_keys[table[0]] = {}

            for pair in pairs:
                if pair[0] == table[0]:
                    flag = True
                    foreign_keys[table[0]][pair[1]] = (pair[2], pair[3])
                    pairs.remove(pair)
                elif pair[2] == table[0]:
                    flag = True
                    foreign_keys[table[0]][pair[3]] = (pair[0], pair[1])
                    pairs.remove(pair)

            if not flag:
                foreign_keys.pop(table[0])

    return foreign_keys


def construct_schema_types(db):
    """
    Construct the schema types dictionary from the given database.

    Args:
        db (dict): The database dictionary.

    Returns:
        dict: The schema types dictionary representing the tables and their column types.
    """
    schema_types = {}
    for index, table_name in enumerate(db["table_names_original"]):
        columns_for_table = [
            column for column in db["column_names_original"] if column[0] == index
        ]
        schema_types[table_name] = {
            column[1]: db["column_types"][db["column_names_original"].index(column)]
            for column in columns_for_table
        }
    return schema_types


def read_schema_pk_fk_types(db_name, file_name, all_db=None, col_exp=False):
    """
    Read the schema, primary keys, foreign keys, and schema types for a given database.

    Args:
        db_name (str): The name of the database.
        file_name (str): The name of the file containing the schema information.

    Returns:
        tuple: A tuple containing the schema, primary keys, foreign keys, and schema types.
    """
    if all_db is None:
        all_db = convert_json_to_schema(file_name, col_exp=col_exp)
    schema = all_db[db_name]["schema"]
    pk = all_db[db_name]["primary_keys"]
    fk = all_db[db_name]["foreign_keys"]
    schema_types = all_db[db_name]["schema_types"]
    if col_exp:
        schema_desc = all_db[db_name]["schema_desc"]
        return schema, pk, fk, schema_types, schema_desc

    return schema, pk, fk, schema_types


# all = convert_json_to_schema(
#     {os.path.abspath("query_generator_single_schema/spider/tables.json")}
# )
