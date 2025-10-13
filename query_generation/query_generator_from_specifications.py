import csv
import json
import os
import random
import argparse
import pickle
import nltk
import openai
from openai import OpenAI
from json_repair import repair_json
# Import functions from different modules
from group_by_having import complete_with_group_by_clause
from having import complete_with_having_clause
from helper_funcs import print_attributes, write_queries_to_file
from limit import complete_query_with_limit
from order_by import complete_query_with_order_by
from parser_sql.parse_sql_one import   get_sql, get_schema
from read_schema import read_schema_pk_fk_types
from read_schema.read_schema import convert_json_to_schema
from select_query import complete_query_with_select
from specification_generator_using_ht import complete_specs
from tqdm import tqdm 
from table_expression import create_table_expression
from where import complete_with_where_clause
import glob
import pandas as pd
from langchain.memory import ConversationBufferMemory

nltk.download('punkt_tab')
class Schema3:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema):
        self._schema = schema
        self._idMap = self._map(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema):
        idMap = {'*': "__all__"}
        id = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap

def query_generator_single_schema(
    db_name,
    schema,
    pk,
    fk,
    schema_types,
    specs=None,
    max_num=1000,
    must_be_in_select=None,
    must_be_in_where=None,
    write_to_csv=True,
    is_subquery=False,
    random_choice=False,
    return_select_fields=False,
    return_table_exp_attributes=False,
    return_unique_tables=False,
    return_select_fields_dict=False,
    for_cte=None,
    rename_must_be_in_select=False,
):
    """
    Generate queries based on the specifications provided in the specs dictionary.

    Args:
        db_name (str): The name of the database.
        schema (dict): The schema of the database.
        pk (list): The primary key columns.
        fk (list): The foreign key columns.
        schema_types (dict): The data types of the schema.
        specs (dict, optional): The specifications for generating queries. Defaults to None.
        max_num (int, optional): The maximum number of queries to generate. Defaults to 1000.
        must_be_in_select (list, optional): The attributes that must be included in the SELECT clause. Defaults to None.
        must_be_in_where (list, optional): The attributes that must be included in the WHERE clause. Defaults to None.
        write_to_csv (bool, optional): Whether to write the generated queries to a CSV file. Defaults to True.
        is_subquery (bool, optional): Whether the generated queries are subqueries. Defaults to False.
        random_choice (bool, optional): Whether to use random choice for certain query components. Defaults to False.

    Returns:
        dict: A dictionary containing the generated queries.
    """
    print("Start reading specifications")
    testing_with_one_spec = False
    if specs!={}:
        testing_with_one_spec = True
    if not testing_with_one_spec:
        print("Testing with multiple specifications")
        current_dir = os.path.dirname(__file__)
        file_name = os.path.join(current_dir, f"output/specs/{db_name}.json")
        with open(file_name) as json_file:
            specs = json.load(json_file)
    else:
        print("Testing with one specification")
        print(specs)

    print("Start generating queries")
    merged_queries = {}
    return_select_fields_dict = {}
    schema_copy = schema.copy()
    pk_copy = pk.copy()
    fk_copy = fk.copy()
    schema_types_copy = schema_types.copy()
    not_parsed_queries = {}

    for i, hash in enumerate(specs[db_name]):
        # Need to have a copy of the schema, pk, fk, schema_types because they are modified in the create_table_expression function
        schema = schema_copy.copy()
        pk = pk_copy.copy()
        fk = fk_copy.copy()
        schema_types = schema_types_copy.copy()
        print(specs[db_name][hash])
        print("************ SET OP ************")
        if "set_op_type" not in specs[db_name][hash]:
            spec = specs[db_name][hash]
        elif specs[db_name][hash]["set_op_type"] == "none":
            spec = specs[db_name][hash]["first_query"]
        else:
            spec = specs[db_name][hash]
            print("************ SET OP ************")

            spec1 = specs[db_name][hash]["first_query"]
            spec2 = specs[db_name][hash]["second_query"]
            try:
                first_query = query_generator_single_schema(
                    db_name,
                    schema,
                    pk,
                    fk,
                    schema_types,
                    specs={
                        db_name: {hash: {"set_op_type": "none", "first_query": spec1}}
                    },
                    write_to_csv=False,
                    is_subquery=False,
                    random_choice=True,
                )

                second_query = query_generator_single_schema(
                    db_name,
                    schema,
                    pk,
                    fk,
                    schema_types,
                    specs={
                        db_name: {hash: {"set_op_type": "none", "first_query": spec2}}
                    },
                    write_to_csv=False,
                    is_subquery=False,
                    random_choice=True,
                )
                first_query = list(first_query.values())[0].split("\n")[0]
                second_query = list(second_query.values())[0].split("\n")[0]
                completed_query = f"{first_query} {spec['set_op_type']} {second_query}"
                print("******************COMPLETED QUERY******************")
                print(completed_query)
                if str(spec) in merged_queries:
                    merged_queries[str(spec)] += "\n" + completed_query
                else:
                    merged_queries[str(spec)] = completed_query
                if write_to_csv:
                    write_queries_to_file(
                        merged_queries=merged_queries, db_name=db_name
                    )
            except Exception as e:
                print(e)
                print("Error in SET OP")
                if str(spec) in merged_queries:
                    merged_queries[str(spec)] += "\n" + e
                else:
                    merged_queries[str(spec)] = e

                if testing_with_one_spec:
                    raise e
                continue

            print("Done generating queries")
            continue

        table_exp_type = spec["table_exp_type"]
        where_clause_type = spec["where_type"]
        group_by_clause_type = spec["number_of_value_exp_in_group_by"]
        having_type = spec["having_type"]
        order_by_type = spec["orderby_type"]
        limit_type = spec["limit_type"]
        value_exp_types = spec["value_exp_types"]
        meaningful_joins = spec["meaningful_joins"]
        distinct = spec["distinct_type"]
        min_max_depth_in_subquery = spec["min_max_depth_in_subquery"]
        if is_subquery:
            random_choice = True

        try:
            queries_with_attributes = create_table_expression(
                schema,
                pk,
                fk,
                schema_types,
                table_exp_type,
                meaningful_joins,
                db_name=db_name,
                random_choice=random_choice,
                min_max_depth_in_subquery=min_max_depth_in_subquery,
                query_generator_single_schema_func=query_generator_single_schema,
            )
            random.shuffle(queries_with_attributes)

        except Exception as e:
            print(e)
            print("Error in table expression")
            if str(spec) in merged_queries:
                merged_queries[str(spec)] += "\n" + e
            else:
                merged_queries[str(spec)] = e

            if testing_with_one_spec:
                raise e
            continue

        for query_info in queries_with_attributes:
            partial_query, tables, attributes, cte = query_info
            print("************TABLE EXPRESSION ************\n")
            print_attributes(
                partial_query=partial_query,
                tables=tables,
                attributes=attributes,
                cte=cte,
            )

            try:
                partial_query_with_attributes = complete_with_where_clause(
                    schema,
                    schema_types,
                    db_name,
                    partial_query,
                    attributes,
                    where_clause_type,
                    pk,
                    fk,
                    tables,
                    must_be_in_where,
                    random_choice=random_choice,
                    min_max_depth_in_subquery=min_max_depth_in_subquery,
                    query_generator_single_schema_func=query_generator_single_schema,
                )
                print("************ WHERE ************")
                for partial_query, attributes in partial_query_with_attributes:
                    print("************ WHERE ************")
                    print_attributes(
                        partial_query=partial_query,
                        tables=tables,
                        attributes=attributes,
                    )

                    try:
                        partial_query_with_attributes = complete_with_group_by_clause(
                            partial_query,
                            attributes,
                            tables,
                            pk,
                            group_by_clause_type,
                            random_choice=random_choice,
                        )
                        print("************ GROUP BY ALL ************")
                        for (
                            partial_query,
                            attributes,
                            must_have_attributes,
                        ) in partial_query_with_attributes:
                            print("************ GROUP BY ************")
                            if must_be_in_select is None:
                                must_be_in_select = []
                            temp = must_be_in_select.copy()
                            for attr in must_have_attributes:
                                temp.append(attr)
                            print_attributes(
                                partial_query=partial_query,
                                attributes=attributes,
                                must_be_in_select=temp,
                            )

                            must_be_in_select1 = temp.copy()
                            try:
                                partial_query_with_attributes = complete_with_having_clause(
                                    partial_query,
                                    attributes,
                                    must_be_in_select1,
                                    having_type,
                                    schema,
                                    schema_types,
                                    db_name,
                                    pk,
                                    fk,
                                    tables,
                                    min_max_depth_in_subquery=min_max_depth_in_subquery,
                                    query_generator_single_schema_func=query_generator_single_schema,
                                    random_choice=random_choice,
                                )

                                for (
                                    partial_query,
                                    attributes,
                                    must_be_in_select1,
                                ) in partial_query_with_attributes:
                                    print("************ Having ************")
                                    print_attributes(
                                        partial_query=partial_query,
                                        attributes=attributes,
                                        must_be_in_select=must_be_in_select1,
                                    )

                                    try:
                                        partial_query_with_attributes = complete_query_with_select(
                                            schema,
                                            schema_types,
                                            db_name,
                                            pk,
                                            fk,
                                            tables,
                                            partial_query,
                                            attributes,
                                            must_be_in_select1,
                                            value_exp_types,
                                            distinct,
                                            is_subquery=is_subquery,
                                            random_choice=random_choice,
                                            min_max_depth_in_subquery=min_max_depth_in_subquery,
                                            query_generator_single_schema_func=query_generator_single_schema,
                                            cte=cte,
                                            rename_must_be_in_select=rename_must_be_in_select,
                                        )
                                        print("************ SELECT ************")
                                        for (
                                            partial_query,
                                            attributes,
                                            must_be_in_select1,
                                            select_clause,
                                            num_value_exps,
                                            select_fields_types,
                                        ) in partial_query_with_attributes:
                                            print_attributes(
                                                partial_query=partial_query,
                                                attributes=attributes,
                                                must_be_in_select=must_be_in_select1,
                                                select_clause=select_clause,
                                                num_value_exps=num_value_exps,
                                                select_fields_types=select_fields_types,
                                            )
                                            print("WHY")

                                            partial_query = (
                                                complete_query_with_order_by(
                                                    partial_query,
                                                    attributes,
                                                    select_clause,
                                                    num_value_exps,
                                                    order_by_type,
                                                )
                                            )
                                            print("************ ORDER BY ************")
                                            print_attributes(
                                                partial_query=partial_query
                                            )

                                            partial_query = complete_query_with_limit(
                                                partial_query, limit_type
                                            )
                                            print(
                                                "************ LIMIT & OFFSET ************"
                                            )
                                            print_attributes(
                                                partial_query=partial_query
                                            )
                                            print("************ COMPLETED QUERY ************")

                                            # current_dir = os.path.dirname(__file__)

                                            table_file = "data/tables.json"
                                            
                                            
                                           
                                            flag = False
                                            db2 = db_name
                                            db2 = os.path.join("test-suite-sql-eval-master/database/", db2, db2 + ".sqlite")
                                            print(db2)
                                            schema2 = Schema3(get_schema(db2))
                                            print(schema2)
                                            print(partial_query)
                                            print(is_subquery)
                                            if not is_subquery:
                                                try:
        
                                                    g_sql = get_sql(schema2, partial_query)

                                                    flag = True
                                                    print(flag)

                                                except AssertionError as e:
                                                    print("Error in Parsing:", e.args)
                                                    if str(spec) in not_parsed_queries:
                                                        not_parsed_queries[
                                                            str(spec)
                                                        ].append((partial_query, e.args))
                                                    else:

                                                        not_parsed_queries[str(spec)] = [
                                                            (partial_query, e.args)
                                                        ]
                                            if is_subquery:
                                                flag = True

                                            if str(spec) in merged_queries and flag:
                                                merged_queries[str(spec)] += (
                                                    "\n\n" + partial_query
                                                )
                                            elif (
                                                str(spec) not in merged_queries and flag
                                            ):
                                                merged_queries[str(spec)] = (
                                                    partial_query
                                                )
                                            if return_select_fields:
                                                return_select_fields_dict[hash] = {
                                                    "select_fields": select_clause
                                                }
                                            if return_table_exp_attributes:
                                                return_select_fields_dict[hash][
                                                    "table_exp_attributes"
                                                ] = {}
                                                return_select_fields_dict[hash][
                                                    "table_exp_attributes"
                                                ] = attributes
                                            if return_unique_tables:
                                                return_select_fields_dict[hash][
                                                    "unique_tables"
                                                ] = tables
                                            if return_select_fields_dict:
                                                return_select_fields_dict[hash][
                                                    "select_fields_types"
                                                ] = select_fields_types
                                    except Exception as e:
                                        print("Error in SELECT")

                                        if str(spec) in merged_queries:
                                            merged_queries[str(spec)] += "\n" + str(e)
                                        else:
                                            merged_queries[str(spec)] = str(e)

                                        if testing_with_one_spec:
                                            raise e
                                        continue

                            except Exception as e:
                                print("Error in HAVING")

                                if str(spec) in merged_queries:
                                    merged_queries[str(spec)] += "\n" + str(e)
                                else:
                                    merged_queries[str(spec)] = str(e)

                                if testing_with_one_spec:
                                    raise e
                                continue

                    except Exception as e:
                        print("Error in GROUP BY")

                        if testing_with_one_spec:
                            raise e
                        continue
            except Exception as e:
                print("Error in WHERE")
                if str(spec) in merged_queries:
                    merged_queries[str(spec)] += "\n" + str(e)
                else:
                    merged_queries[str(spec)] = str(e)

                if testing_with_one_spec:
                    raise e
                continue
    if write_to_csv:
        output_dir = "data/synthetic-queries/schema_guided/"
        csv_file = output_dir+ f"{db_name}_res.csv"
        print(csv_file)
        print(merged_queries)
        write_queries_to_file(
            merged_queries=merged_queries, db_name=db_name, file_name=csv_file
        )
        current_dir = os.path.dirname(__file__)
        output_dir = os.path.abspath(os.path.join(current_dir, "output/errors/"))
        error_csv_file = os.path.join(output_dir, f"parsing_error_{db_name}_res.csv")
        write_queries_to_file(
            merged_queries=not_parsed_queries,
            db_name=db_name,
            file_name=error_csv_file,
        )

    print("Done generating queries")
    if return_select_fields:
        return merged_queries, return_select_fields_dict
    return merged_queries


def query_generator(
    db_name=None,
    specs=None,
    max_num=1000,
    config_name="config_file.json",
    write_to_csv=True,
    random_choice=False,
    n_dbs=None,
):
    # current_dir = os.path.dirname(__file__)
    db_file = "data/tables.json"
    all_db = convert_json_to_schema(db_file)
    current_dir = os.path.dirname(__file__)
    config_file = os.path.abspath(os.path.join(current_dir, config_name))
    if n_dbs is None:
        n_dbs = len(all_db)
    if db_name=="":
        # randomly select:
        random_dbs = random.sample(list(all_db.keys()), n_dbs)
        # call query_generator_single_schema for all databases
        for db in random_dbs:
            schema, pk, fk, schema_types = read_schema_pk_fk_types(
                db, db_file, all_db=all_db
            )
            complete_specs(
                db_file,
                config_file,
                db_name=db,
                num_query=max_num,
            )
            print("DONEEEEEEEEE_________________________________-")
            try:
                query_generator_single_schema(
                    db,
                    schema,
                    pk,
                    fk,
                    schema_types,
                    specs=specs,
                    max_num=20,
                    write_to_csv=True,
                    random_choice=random_choice,
                )
            except Exception as e:
                print(e)
    else:
        # call query_generator_single_schema for the given database
        schema, pk, fk, schema_types = read_schema_pk_fk_types(
            db_name, db_file, all_db=all_db
        )
        complete_specs(
            db_file,
            config_file,
            db_name=db_name,
            num_query=max_num,
        )

        try:
            query_generator_single_schema(
                db_name,
                schema,
                pk,
                fk,
                schema_types,
                specs=specs,
                max_num=max_num,
                write_to_csv=True,
                random_choice=random_choice,
            )
        except Exception as e:
            print(e)

def evaluate_query(query, db_name, schema, specification):
        # using llm to check the query
        pass


def is_duplicate(template, history, db_name):
    for entry in history[db_name]:
        if entry== template:
            return True
    return False
 

def make_request(messages,client, model_name):
        response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            max_tokens=512,
    )
        print("RESPONSE ", response)
        good_json_string = repair_json(response.choices[0].message.content,return_objects=True)
        return good_json_string
def save_checkpoint_llm(filename, data):
    """Save the checkpoint to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def load_checkpoint_llm(filename):
    """Load the checkpoint from a JSON file if it exists."""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}
def query_generator_single_schema_llm(client,model_name, db_name, schema, pk, fk, schema_types,all_db, specs, max_num=20, write_to_csv=True, random_choice=True,    checkpoint_file="data/synthetic-queries/llm_based/checkpoint.json",
):        
    checkpoint_data = load_checkpoint_llm(checkpoint_file)
    chat_history = checkpoint_data.get("chat_history", {})
    queries = checkpoint_data.get("queries", {})
    few_shot_examples = [
        {"role":"system",
         "content":"""You are an SQL query generator. Your task is to generate an SQLite query based on the given specification. 
Follow the specification details closely to construct a valid and accurate query. Do not use INNER JOIN.
The specification includes schema details, primary keys (PK), foreign keys (FK), and column types. 

The output must be in JSON format and include two keys: 
1. 'query': This contains the generated SQL query as a valid string.
2. 'template': This is the normalized representation of the query used to capture its structural uniqueness.

For the 'template', adhere to the following rules:
- Replace all **table names** with the placeholder `#TABLE#`.
- Replace all **column names** with the placeholder `#COLUMN#`.
- Replace all **literal values** (e.g., strings, integers, floats) with the placeholder `#VALUE#`.
- If the query uses aliases (e.g., `T1` or `T2`), remove the alias prefix (e.g., `T1.Name` becomes `#COLUMN#`) and remove the alias like AS T2 to "".
- Preserve SQL syntax, keywords (e.g., `SELECT`, `JOIN`, `WHERE`), and structural components exactly as they are.
- Ensure the structure remains valid and consistent for comparisons of uniqueness.
"""
         
         
         },
    # Example 1
    {
        "role": "user",
        "content": (
            '{"spec": {"meaningful_joins": "yes", "table_exp_type": "single_table", "where_type": "none", '
            '"number_of_value_exp_in_group_by": 0, "having_type": "none", "orderby_type": "DESC", "limit_type": "none", '
            '"value_exp_types": "*", "distinct_type": "none", "min_max_depth_in_subquery": [1, 1]}, '
            '"schema": {"table_name": "Ref_Incident_Type", "columns": ["incident_type_code", "incident_description"]}, '
            '"pk": ["incident_type_code"], "fk": [], "column_types": {"incident_type_code": "integer", "incident_description": "text"}}'
        ),
    },
    {
        "role": "assistant",
        "content": (
            '{"query": "SELECT * FROM Ref_Incident_Type ORDER BY incident_type_code DESC", '
            '"template": "SELECT #COLUMN# FROM #TABLE# ORDER BY #COLUMN# DESC"}'
        ),
    },
    
    # Example 2
    {
        "role": "user",
        "content": (
            '{"spec": {"meaningful_joins": "yes", "table_exp_type": "multi_table", "where_type": {"logical_operator": ["AND"]}, '
            '"number_of_value_exp_in_group_by": 1, "having_type": "SUM", "orderby_type": "ASC", "limit_type": "with_offset", '
            '"value_exp_types": ["agg_exp"], "distinct_type": "none", "min_max_depth_in_subquery": [1, 1]}, '
            '"schema": {"table_name": "Employee", "columns": ["id", "salary", "department"], '
            '"join_condition": "Employee.department_id = Department.id", '
            '"fk": ["department_id"], "column_types": {"id": "integer", "salary": "float", "department": "text"}}, '
            '"join_table": {"table_name": "Department", "columns": ["id", "name"], "pk": ["id"], '
            '"column_types": {"id": "integer", "name": "text"}}'
        ),
    },
    {
        "role": "assistant",
        "content": (
            '{"query": "SELECT d.name, SUM(e.salary) FROM Employee e '
            'JOIN Department d ON e.department_id = d.id '
            'GROUP BY d.name HAVING SUM(e.salary) > 10000 OFFSET 5", '
            '"template": "SELECT #COLUMN#, SUM(#COLUMN#) FROM #TABLE# JOIN #TABLE# ON #COLUMN# = #COLUMN# '
            'GROUP BY #COLUMN# HAVING SUM(#COLUMN#) > #VALUE# OFFSET #VALUE#"}'
        ),
    },
    {
        "role": "user",
        "content": (
            '{"spec": {"meaningful_joins": "yes", "table_exp_type": "multi_table", "where_type": {"logical_operator": ["<"]}, '
            '"number_of_value_exp_in_group_by": 0, "having_type": "none", "orderby_type": "none", "limit_type": "none", '
            '"value_exp_types": ["basic_comparison"], "distinct_type": "none", "min_max_depth_in_subquery": [1, 1]}, '
            '"schema": {"table_name": "Manufacturers", "columns": ["Code", "Name", "Revenue"], '
            '"join_condition": "Manufacturers.Code = Products.Manufacturer", '
            '"fk": ["Manufacturer"], "column_types": {"Code": "integer", "Name": "text", "Revenue": "float"}}, '
            '"join_table": {"table_name": "Products", "columns": ["Manufacturer", "Price"], "pk": ["Manufacturer"], '
            '"column_types": {"Manufacturer": "integer", "Price": "float"}}'
        ),
    },
    {
        "role": "assistant",
        "content": (
            '{"query": "SELECT T1.Name, T2.Price FROM Manufacturers AS T1 '
            'JOIN Products AS T2 ON T1.Code = T2.Manufacturer WHERE T1.Revenue < 10000", '
            '"template": "SELECT #COLUMN#, #COLUMN# FROM #TABLE# JOIN #TABLE# ON #COLUMN# = #COLUMN# WHERE #COLUMN# < #VALUE#"}'
        ),
    }
]



# Chat history stored in a Python list

# Function to check for duplicate templates


# Function to interact with OpenAI API
    # messages = few_shot_examples + [
    #     {"role": "system", "content": json.dumps({"spec": spec})}
    # ]
    # res=make_request(messages,client, model_name)
    # query = res.get('query', None)
    # template= res.get('template', None)

    
    # # Check for duplicates
    # if is_duplicate(template, chat_history):
    #     return None
    # else:
    #     # Add the result to the chat history
    #     chat_history.append(template)
    #     return query



    print("Start reading specifications")
    testing_with_one_spec = False
    if specs!={}:
        testing_with_one_spec = True
    if not testing_with_one_spec:
        print("Testing with multiple specifications")
        current_dir = os.path.dirname(__file__)
        file_name = os.path.join(current_dir, f"output/specs/{db_name}.json")
        with open(file_name) as json_file:
            specs = json.load(json_file)
    else:
        print("Testing with one specification")
        print(specs)

    print("Start generating queries")

    schema_copy = schema.copy()
    pk_copy = pk.copy()
    fk_copy = fk.copy()
    schema_types_copy = schema_types.copy()

    if db_name not in queries:
        queries[db_name]=[]




    if db_name not in chat_history:
        chat_history[db_name]=[]
    print("______________________",len(specs[db_name]))

    processed_hashes = {spec["hash"] for spec in queries[db_name]}  # Extract already processed hashes

    for i, hash in enumerate(tqdm(specs[db_name], desc=f"Processing {db_name}", unit="query")):
        # Need to have a copy of the schema, pk, fk, schema_types because they are modified in the create_table_expression function
        print(hash)
        print(specs[db_name][hash])
        if hash in processed_hashes:
            print("ALREDy")
            continue
        schema = schema_copy.copy()
        pk = pk_copy.copy()
        fk = fk_copy.copy()
        schema_types = schema_types_copy.copy()
        messages = few_shot_examples + [
          { "role": "user",
            "content": f"""{{"spec":{specs[db_name][hash]}, "schema": {schema},"pk": {pk} ,"fk": {fk}, "column_types": {schema_types}}}"""    }
    ]
        
       
        res = make_request(messages,client, model_name)
        print("RES: ",res)
        try:
            query = res.get('query', None)
            template= res.get('template', None)
        except:
            continue
        print("query",query)
        print("template", template)
        print("history",chat_history)

    
    # # Check for duplicates
        if is_duplicate(template, chat_history, db_name):
            print("Duplicate template found. Regenerating...")
            continue
        else:
            # Add the result to the chat history
            chat_history[db_name].append(template)
            print("HI")
            queries[db_name].append({"hash": hash, "query": query, "template": template})
            print("BYEE")
            save_checkpoint_llm(checkpoint_file, {"chat_history": chat_history, "queries": queries})
            print("SEVED!")
            print("DB_NAME:", db_name)

    return queries

def generate_query_using_llm(max_num, random_choice, config_name,db_name=None, specs=None, write_to_csv=False ):
        db_file = "data/tables.json"
        current_dir = os.path.dirname(__file__)
        all_db = convert_json_to_schema("data/tables.json", col_exp=False)
        n_dbs = len(all_db)
        config_file = os.path.abspath(os.path.join(current_dir, config_name))
        client = OpenAI(
        api_key="token-wdmuofa",
        base_url="http://anagram.cs.ualberta.ca:2000/v1" # Choose one from the table
        # base_url = "http://turin4.cs.ualberta.ca:2001/v1"
        
    )
     
        model_name = "meta-llama/Meta-Llama-3.1-70B-Instruct"
    
    
        if db_name=="":
            # randomly select:
            random_dbs = random.sample(list(all_db.keys()), n_dbs)
            # call query_generator_single_schema for all databases
            for db in random_dbs:
                schema, pk, fk, schema_types = read_schema_pk_fk_types(db, db_file, all_db=all_db, col_exp=False)
                complete_specs(db_file, config_file, db_name=db, num_query=max_num)
                try:
                    queries = query_generator_single_schema_llm(client,model_name,db, schema, pk, fk, schema_types,all_db, specs=specs, max_num=max_num, write_to_csv=True, random_choice=random_choice)
                    print(queries)
                    save_queries_to_csv(queries, output_dir="data/synthetic-queries/llm_based")


                except Exception as e:
                    print(e)
        else:
            # call query_generator_single_schema for the given database
            print(db_name)
            schema, pk, fk, schema_types = read_schema_pk_fk_types(db_name, db_file, all_db=all_db)
            complete_specs(db_file, config_file, db_name=db_name, num_query=max_num)
            try:
                queries = query_generator_single_schema_llm(client,model_name,db_name, schema, pk, fk, schema_types,all_db, specs=specs, max_num=max_num, write_to_csv=True, random_choice=random_choice)
            except Exception as e:
                print(e)
        if write_to_csv:
            save_queries_to_csv(queries, output_dir="data/synthetic-queries/llm_based")



def save_queries_to_csv(queries_dict, output_dir="data/synthetic-queries/Schema_guided_llm_refinement"):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Iterate through each database name and its list of queries
    for db_name, queries in queries_dict.items():
        # Define the CSV file path for the current db_name
        file_path = os.path.join(output_dir, f"{db_name}.csv")

        # Write the queries to the CSV file
        with open(file_path, mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["query"])  # Header row
            for query in queries:
                writer.writerow([query])

        print(f"Saved {len(queries)} queries to {file_path}")    


def query_generator_with_llm_refinement(
    db_name=None,
    specs=None,
    max_num=1000,
    config_name="config_file.json",
    random_choice=False,
    folder_path="data/synthetic-queries/schema_guided/",
    k=3,
    checkpoint_path="data/synthetic-queries/schema_guided_llm_refinement/checkpoint.pkl",
):
    # Load or initialize checkpoint
    checkpoint_dir = os.path.dirname(checkpoint_path)
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir, exist_ok=True)

    checkpoint = load_or_initialize_checkpoint(checkpoint_path)
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' not found. Creating folder and generating queries...")
        os.makedirs(folder_path, exist_ok=True)
        query_generator(
            db_name=db_name,
            specs=specs,
            max_num=max_num,
            write_to_csv=True,
            random_choice=random_choice,
            config_name=config_name,
        )

    logs, queries, total, refinement_summary, still_not_good, processed_files = (
        checkpoint["logs"],
        checkpoint["queries"],
        checkpoint["total"],
        checkpoint["refinement_summary"],
        checkpoint["still_not_good"],
        checkpoint["processed_files"],
    )
    # Load database schema information
    all_db = convert_json_to_schema(
        "data/tables.json", col_exp=False
    )

    # Initialize LLM client
    client = OpenAI(
        api_key="token-wdmuofa",
        base_url="http://anagram.cs.ualberta.ca:2000/v1"
    )
    model_name = "meta-llama/Meta-Llama-3.1-70B-Instruct"

    # Iterate through CSV files for processing
    print(glob.glob(os.path.join(folder_path, "*.csv")))
    for file in tqdm(glob.glob(os.path.join(folder_path, "*.csv")), desc="Processing files"):
        db_name = os.path.basename(file).split("_res.csv")[0]

        # Skip already processed files
        if db_name in processed_files:
            continue

        # Initialize logs and queries for the current db_name if not already done
        logs.setdefault(db_name, [])
        queries.setdefault(db_name, [])

        # Read the CSV file and skip already processed queries
        df = pd.read_csv(file)
        start_index = len(queries[db_name])
        df = df[start_index:]

        # Iterate over the queries in the dataframe starting from the point where we left off
        for instance in tqdm(df.to_dict(orient="records"), desc=f"Processing queries for {db_name}"):
            query = instance["query"]
            schema = all_db[db_name]["schema"]
            pk = all_db[db_name]["primary_keys"]
            fk = all_db[db_name]["foreign_keys"]
            col_types = all_db[db_name]["schema_types"]

            total += 1

            # Generate feedback for the initial query
            feedback = generate_feedback(query, schema, pk, fk, col_types, client, model_name)
            print(feedback)

            # Handle feedback results
            if feedback == "valid":
                queries[db_name].append(query)
            else:
                refined_query, flag = refine_query(query, feedback, schema, pk, fk, col_types, client, model_name, k)

                if flag:
                    refinement_summary[str(flag)] += 1
                    logs[db_name].append([query, refined_query])
                    queries[db_name].append(refined_query)
                else:
                    queries[db_name].append(refined_query)
                    still_not_good += 1

            # Save checkpoint after processing each query
            save_checkpoint(
                checkpoint_path,
                logs,
                queries,
                total,
                refinement_summary,
                still_not_good,
                processed_files,
            )

        # Mark the current file as processed
        processed_files.add(db_name)

        # Save processed queries to CSV after completing the processing for the current db_name
        save_processed_queries_to_csv(queries[db_name], db_name)
        save_checkpoint(
                checkpoint_path,
                logs,
                queries,
                total,
                refinement_summary,
                still_not_good,
                processed_files,
            )

    # Save final results
    # save_queries_to_csv(
    #     queries, output_dir="data/synthetic-queries/schema-guided-llm-refinement"
    # )
    save_logs(
        logs,
        refinement_summary,
        still_not_good,
        total,
        output_file="data/synthetic-queries/schema_guided_llm_refinement/logs.txt",
    )


### Helper Functions ###

def load_or_initialize_checkpoint(checkpoint_path):
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, "rb") as f:
            return pickle.load(f)
    else:
        return {
            "logs": {},
            "queries": {},
            "total": 0,
            "refinement_summary": {"1": 0, "2": 0, "3": 0, "4": 0},
            "still_not_good": 0,
            "processed_files": set(),
        }

def generate_feedback(query, schema, pk, fk, col_types, client, model_name):

    feedback_msgs = [
    {
        "role": "system",
        "content": "You are an SQL query evaluator. Your task is to evaluate the provided SQL query, ensuring it is technically correct and relevant based on schema details including primary keys, foreign keys, and column types. Provide your feedback in JSON format under the single key 'feedback'. If valid, return {'feedback': 'valid'}. Otherwise, provide specific feedback."
    },
  
    {
        "role": "user",
        "content": (
            "Example 1:\n"
            "Query: SELECT Name FROM ship WHERE Ship_ID = 5;\n"
            "Schema Info:\n"
            "  schema = {'mission': ['Mission_ID', 'Ship_ID', 'Code', 'Launched_Year', 'Location', 'Speed_knots', 'Fate'], 'ship': ['Ship_ID', 'Name', 'Type', 'Nationality', 'Tonnage']}\n"
            "  pk = {'mission': 'Mission_ID', 'ship': 'Ship_ID'}\n"
            "  fk = {'mission': {'Ship_ID': ('ship', 'Ship_ID')}}\n"
            "  col_types = {'mission': {'Mission_ID': 'number', 'Ship_ID': 'number', 'Code': 'text', 'Launched_Year': 'number', 'Location': 'text', 'Speed_knots': 'number', 'Fate': 'text'}, 'ship': {'Ship_ID': 'number', 'Name': 'text', 'Type': 'text', 'Nationality': 'text', 'Tonnage': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'feedback': 'valid'}"
    },
    {
        "role": "user",
        "content": (
            "Example 2:\n"
            "Query: SELECT Name, FROM ship;\n"
            "Schema Info:\n"
            "  schema = {'mission': ['Mission_ID', 'Ship_ID', 'Code', 'Launched_Year', 'Location', 'Speed_knots', 'Fate'], 'ship': ['Ship_ID', 'Name', 'Type', 'Nationality', 'Tonnage']}\n"
            "  pk = {'mission': 'Mission_ID', 'ship': 'Ship_ID'}\n"
            "  fk = {'mission': {'Ship_ID': ('ship', 'Ship_ID')}}\n"
            "  col_types = {'mission': {'Mission_ID': 'number', 'Ship_ID': 'number', 'Code': 'text', 'Launched_Year': 'number', 'Location': 'text', 'Speed_knots': 'number', 'Fate': 'text'}, 'ship': {'Ship_ID': 'number', 'Name': 'text', 'Type': 'text', 'Nationality': 'text', 'Tonnage': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'feedback': {'error': 'Syntax error: Extra comma before FROM clause.', 'suggestion': 'Remove the extra comma before FROM.'}}"
    },
    {
        "role": "user",
        "content": (
            "Example 3:\n"
            "Query: SELECT Mission_ID, COUNT(*) FROM mission GROUP BY Ship_ID;\n"
            "Schema Info:\n"
            "  schema = {'mission': ['Mission_ID', 'Ship_ID', 'Code', 'Launched_Year', 'Location', 'Speed_knots', 'Fate'], 'ship': ['Ship_ID', 'Name', 'Type', 'Nationality', 'Tonnage']}\n"
            "  pk = {'mission': 'Mission_ID', 'ship': 'Ship_ID'}\n"
            "  fk = {'mission': {'Ship_ID': ('ship', 'Ship_ID')}}\n"
            "  col_types = {'mission': {'Mission_ID': 'number', 'Ship_ID': 'number', 'Code': 'text', 'Launched_Year': 'number', 'Location': 'text', 'Speed_knots': 'number', 'Fate': 'text'}, 'ship': {'Ship_ID': 'number', 'Name': 'text', 'Type': 'text', 'Nationality': 'text', 'Tonnage': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'feedback': {'error': 'Incorrect GROUP BY: Should group by Mission_ID instead of Ship_ID to match aggregation context.', 'suggestion': 'Replace GROUP BY Ship_ID with GROUP BY Mission_ID.'}}"
    },
    {
        "role": "user",
        "content": (
            f"Now evaluate the following query based on the given schema information.\n"
            f"Query: {query}\n"
            f"Schema Info:\n"
            f"  schema = {schema}\n"
            f"  pk = {pk}\n"
            f"  fk = {fk}\n"
            f"  col_types = {col_types}\n"
        )
    }
]

    try:
        res = make_request(feedback_msgs, client, model_name)
        print(query)
        print(res)
    except openai.BadRequestError as e:
        feedback_msgs = [
    {
        "role": "system",
        "content": "You are an SQL query evaluator. Your task is to evaluate the provided SQL query, ensuring it is technically correct and relevant based on schema details including primary keys, foreign keys, and column types. Provide your feedback in JSON format under the single key 'feedback'. If valid, return {'feedback': 'valid'}. Otherwise, provide specific feedback."
    },

    {
        "role": "user",
        "content": (
            f"Now evaluate the following query based on the given schema information.\n"
            f"Query: {query}\n"
            f"Schema Info:\n"
            f"  schema = {schema}\n"
            f"  pk = {pk}\n"
            f"  fk = {fk}\n"
            # f"  col_types = {col_types}\n"
        )
    }
]
        res = make_request(feedback_msgs, client, model_name)


    if isinstance(res, list):
        # Iterate over the list to find the feedback field
        for item in res:
            if isinstance(item, dict) and 'feedback' in item:
                return item['feedback']
    elif isinstance(res, dict):
        return res.get("feedback", "invalid")
    elif isinstance(res, str):
        return res

    # Default case if feedback is not found
    return "invalid"

def refine_query(query, feedback, schema, pk, fk, col_types, client, model_name, k):
    for i in range(k):
        refine_msgs = [
    {
        "role": "system",
        "content": (
            "You are an SQL query refiner. Refine the given SQL query based on feedback and schema details. "
            "Ensure correctness, logical consistency, and alignment with the schema. "
            "Return the refined query in JSON format with the key 'refined_query'."
        )
    },
    {
        "role": "user",
        "content": (
            "Example 1:\n"
            "Original Query: SELECT name, FROM employees;\n"
            "Feedback: {'error': 'Syntax error: Extra comma before FROM clause.', 'suggestion': 'Remove the extra comma before FROM.'}\n"
            "Schema Info:\n"
            "  schema = {'employees': ['id', 'name', 'department_id']}\n"
            "  pk = {'employees': 'id'}\n"
            "  fk = {'employees': {'department_id': ('departments', 'id')}}\n"
            "  col_types = {'employees': {'id': 'number', 'name': 'text', 'department_id': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'refined_query': 'SELECT name FROM employees;'}"
    },
    {
        "role": "user",
        "content": (
            "Example 2:\n"
            "Original Query: SELECT department_id, COUNT(*) FROM employees GROUP BY department_name;\n"
            "Feedback: {'error': 'Invalid GROUP BY: department_name is not in the schema.', 'suggestion': 'Use department_id for grouping, as department_name is not available in the employees table.'}\n"
            "Schema Info:\n"
            "  schema = {'employees': ['id', 'name', 'department_id']}\n"
            "  pk = {'employees': 'id'}\n"
            "  fk = {'employees': {'department_id': ('departments', 'id')}}\n"
            "  col_types = {'employees': {'id': 'number', 'name': 'text', 'department_id': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'refined_query': 'SELECT department_id, COUNT(*) FROM employees GROUP BY department_id;'}"
    },
    {
        "role": "user",
        "content": (
            "Example 3:\n"
            "Original Query: SELECT Mission_ID, Ship_ID FROM mission WHERE Speed_knots > 20 ORDER BY Name;\n"
            "Feedback: {'error': 'ORDER BY clause contains column Name, which is not part of the mission table.', 'suggestion': 'Use Speed_knots or another valid column from the mission table for ordering.'}\n"
            "Schema Info:\n"
            "  schema = {'mission': ['Mission_ID', 'Ship_ID', 'Code', 'Launched_Year', 'Location', 'Speed_knots', 'Fate'], 'ship': ['Ship_ID', 'Name', 'Type', 'Nationality', 'Tonnage']}\n"
            "  pk = {'mission': 'Mission_ID', 'ship': 'Ship_ID'}\n"
            "  fk = {'mission': {'Ship_ID': ('ship', 'Ship_ID')}}\n"
            "  col_types = {'mission': {'Mission_ID': 'number', 'Ship_ID': 'number', 'Code': 'text', 'Launched_Year': 'number', 'Location': 'text', 'Speed_knots': 'number', 'Fate': 'text'}, 'ship': {'Ship_ID': 'number', 'Name': 'text', 'Type': 'text', 'Nationality': 'text', 'Tonnage': 'number'}}"
        )
    },
    {
        "role": "assistant",
        "content": "{'refined_query': 'SELECT Mission_ID, Ship_ID FROM mission WHERE Speed_knots > 20 ORDER BY Speed_knots;'}"
    },
    {
        "role": "user",
        "content": (
            f"Now refine the following query based on the given feedback and schema information.\n"
            f"Original Query: {query}\n"
            f"Feedback: {feedback}\n"
            f"Schema Info:\n"
            f"  schema = {schema}\n"
            f"  pk = {pk}\n"
            f"  fk = {fk}\n"
            f"  col_types = {col_types}\n"
        )
    }
]
    
        try:
            res = make_request(refine_msgs, client, model_name)
        except openai.BadRequestError as e:
            print("Context window problem:")
            refine_msgs  = [
    {
        "role": "system",
        "content": (
            "You are an SQL query refiner. Refine the given SQL query based on feedback and schema details. "
            "Ensure correctness, logical consistency, and alignment with the schema. "
            "Return the refined query in JSON format with the key 'refined_query'."
        )
    },

    {
        "role": "user",
        "content": (
            f"Now refine the following query based on the given feedback and schema information.\n"
            f"Original Query: {query}\n"
            f"Feedback: {feedback}\n"
            f"Schema Info:\n"
            f"  schema = {schema}\n"
            f"  pk = {pk}\n"
            f"  fk = {fk}\n"
            # f"  col_types = {col_types}\n"
        )
    }
]

            res = make_request(refine_msgs, client, model_name)



        refined_query = query  # Default to original query if no valid refined query is returned
        
        if isinstance(res, list):
            # Iterate over the list to find the refined query field
            for item in res:
                if isinstance(item, dict) and 'refined_query' in item:
                    refined_query = item['refined_query']
                    break
        elif isinstance(res, dict):
            refined_query = res.get("refined_query", query)
        elif isinstance(res, str):
            refined_query = res
        # Get feedback on refined query
        feedback = generate_feedback(refined_query, schema, pk, fk, col_types, client, model_name)
        if feedback == "valid":
            return refined_query, str(i + 1)

    return refined_query, None

def save_checkpoint(checkpoint_path, logs, queries, total, refinement_summary, still_not_good, processed_files):
    with open(checkpoint_path, "wb") as f:
        pickle.dump(
            {
                "logs": logs,
                "queries": queries,
                "total": total,
                "refinement_summary": refinement_summary,
                "still_not_good": still_not_good,
                "processed_files": processed_files,
            },
            f,
        )

def save_logs(logs, refinement_summary, still_not_good, total, output_file):
    with open(output_file, "w") as outfile:
        outfile.write(f"Logs: {logs}\n")
        outfile.write(f"Refinement Summary: {refinement_summary}\n")
        outfile.write(f"Still Not Good: {still_not_good}\n")
        outfile.write(f"Total: {total}\n")

def save_processed_queries_to_csv(queries, db_name):
    output_path = f"data/synthetic-queries/schema_guided_llm_refinement/{db_name}_processed.csv"
    df = pd.DataFrame(queries, columns=["query"])
    df.to_csv(output_path, index=False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--db_name", help="Database name", type=str, nargs='?')
    parser.add_argument("--num", help="Number of queries", type=int, nargs='?', default=60)
    parser.add_argument("--write_to_csv", help="Write output to CSV", type=bool, nargs='?', const=True, default=False)
    parser.add_argument("--random_choice", help="Enable random choice", type=bool, nargs='?', const=True, default=True)
    parser.add_argument("--config_name", help="Configuration file name", type=str, nargs='?', default="config_file.json")
    parser.add_argument("--synthesis_method", help="Method to synthesize SQL queries", type=str, choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement'], default='schema_guided')
    parser.add_argument("--spec", help="Specification for query generation (JSON string)", type=str, nargs='?', default="{}")
    parser.add_argument("--refinement_k", help="Max number of iteration for refinement", type=int, nargs='?', default=3, const=3)
    print("HIIIII")
    print(parser)
    args = parser.parse_args()
    try:
        args.spec = json.loads(args.spec)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON for --spec: {e}")
        exit(1)

    db_name_display = 'All Databases' if args.db_name=="" else args.db_name
    print("Database Name:", db_name_display)
    print("Number of Queries:", args.num)
    print("Write to CSV:", args.write_to_csv)
    print("Random Choice:", args.random_choice)
    print("Config Name:", args.config_name)
    print("Synthesis Method:", args.synthesis_method)


    # specs = {
    #     "advertising_agencies": {
    #         "ae99efea2cbadfa5e336d8fd2a4fd91b0911f8b8": {
    #             "set_op_type": "none",
    #             "first_query": {
    #                 'meaningful_joins': 'yes',
    #                 'table_exp_type': 'JOIN',
    #                 'where_type': {'logical_operator': ['OR', 'basic_comparison', 'exists_subquery']},
    #                 'number_of_value_exp_in_group_by': 0,
    #                 'having_type': 'none',
    #                 'orderby_type': 'ASC',
    #                 'limit_type': 'none',
    #                 'value_exp_types': ['agg_exp', 'agg_exp', 'single_exp_text'],
    #                 'distinct_type': 'none',
    #                 'min_max_depth_in_subquery': [1, 1]
    #             }
    #         }
    #     }
    # }

    # Example function call (commented out for now)
    if args.db_name=="":
        print("Generating queries for all databases")
    
    if args.synthesis_method == 'llm_based':
        print("Using LLM-based method to synthesize SQL queries")
        print(f"Number of Queries: {args.num}, Database Name: {db_name_display}, Config File: {args.config_name}")
        generate_query_using_llm(
            db_name=args.db_name,
            specs=args.spec,
            max_num=args.num,
            write_to_csv=args.write_to_csv,
            random_choice=args.random_choice,
            config_name=args.config_name,
        )

    elif args.synthesis_method == 'schema_guided':
        print("Using schema-guided method to synthesize SQL queries")
        print(f"Number of Queries: {args.num}, Database Name: {args.db_name}, Config File: {args.config_name}")
        ###################### Schema-guided query generation ######################
        query_generator(
            db_name=args.db_name,
            specs=args.spec,
            max_num=args.num,
            write_to_csv=args.write_to_csv,
            random_choice=args.random_choice,
            config_name=args.config_name,
        )
        

    elif args.synthesis_method == 'schema_guided_llm_refinement':
        print("Using schema-guided + LLM refinement method to synthesize SQL queries")
        print(f"Number of Queries: {args.num}, Database Name: {args.db_name}, Config File: {args.config_name}")
        ##################### Schema-guided + LLM Refinement query generation ######################
        query_generator_with_llm_refinement(db_name=args.db_name,
            specs=args.spec,
            max_num=args.num,
            random_choice=args.random_choice,
            config_name=args.config_name,
            k=args.refinement_k)
        # pass
