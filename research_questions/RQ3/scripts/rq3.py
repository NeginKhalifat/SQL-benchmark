from pathlib import Path

import json
import random

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[2]
DATA_DIR = SCRIPT_DIR.parent / "data"
ANALYSIS_INPUTS_DIR = "research_questions/RQ3/output/analysis_inputs"
ANALYSIS_INPUTS_DIR.mkdir(parents=True, exist_ok=True)

file_name1 = "data/combined_filtered_llm_based.json"
file_name2 = "data/combined_filtered_schema_guided_llm_refinement.json"
output_file_name = ANALYSIS_INPUTS_DIR / "RQ3.json"
def get_randon_examples(file_name):
    with open(file_name,"r")as file:
        data = json.load(file)


    categories = {'extra': [], 'hard': [], 'medium': [], 'easy': []}
    for item in data:
        if item['hardness'] in categories:
            categories[item['hardness']].append(item)

    # Get 5 random examples from each category
    random_examples = []
    for category in categories:
        random_examples.extend(random.sample(categories[category], 5))
    return random_examples


import glob
import json
import os
import pickle

import pandas as pd
from dotenv import load_dotenv, find_dotenv

from tqdm import tqdm
from json_repair import repair_json
# from read_schema.read_schema import convert_json_to_schema


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





# all = convert_json_to_schema(
#     {os.path.abspath("query_generator_single_schema/spider/tables.json")}
# )

load_dotenv(find_dotenv()  )
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
from openai import OpenAI

client = OpenAI()

from tqdm import tqdm
import pandas as pd

def read_schema_pk_fk_types(db_name, file_name, all_db=None):
    """
    Read the schema, primary keys, foreign keys, and schema types for a given database.

    Args:
        db_name (str): The name of the database.
        file_name (str): The name of the file containing the schema information.

    Returns:
        tuple: A tuple containing the schema, primary keys, foreign keys, and schema types.
    """
    if all_db is None:
        all_db = convert_json_to_schema(file_name)
    schema = all_db[db_name]["schema"]
    pk = all_db[db_name]["primary_keys"]
    fk = all_db[db_name]["foreign_keys"]
    schema_types = all_db[db_name]["schema_types"]
    return schema, pk, fk, schema_types


def make_request(messages,client, model_name):
    response = client.chat.completions.create(
        model=model_name,
        messages=messages,
        max_tokens=100,
 
    )
    # good_json_string = repair_json(response.choices[0].message.content,return_objects=True)

    return response.choices[0].message.content
if __name__ == "__main__":
    openai_api_key = "EMPTY"
    openai_api_base = "http://anagram.cs.ualberta.ca:8000/v1"

    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )

    model_name = "meta-llama/Meta-Llama-3.1-70B"

    # good_json_string = make_request("hi",client,model_name)

    random_examples = get_randon_examples(file_name1)
    random_examples = random_examples + get_randon_examples(file_name2)
    print(len(random_examples))


    with open(output_file_name, "w") as output_file:
        json.dump(random_examples, output_file, indent=4)
    OUTPUT_FILE = "research_questions/RQ3/outputs/simple_nlq_convertor.csv"
    dataset = random_examples
    test_df = dataset
    print("Number: ", len(test_df))

    # Initialize the progress bar with the total number of rows.
    pbar = tqdm(total=len(test_df), desc="Processing queries")
    header = ['Gold SQL','Predicted NLQ','Back translation NLQ',   'Database','Hardness']
    pd.DataFrame(columns=header).to_csv(OUTPUT_FILE, index=False, mode='w')  # Write header for CSV

    for index, row in enumerate(test_df):
        # if index<63:
        #     continue
        # break
        print(f"index is {index}")
        print(row['query'])
        print(row['question'])
        db_name = row["db_id"]
        schema, pk, fk, schema_types = read_schema_pk_fk_types(
            db_name, "data/tables.json"
        )
        sql = row["query"]

        pbar.update(1)
        messages = [
    {
        "role": "system",
        "content": f"""You are tasked with generating a natural language question for the following SQL query:

SQL Query:
{sql}

Database Information:
- Schema: {schema}
- Primary Keys: {pk}
- Foreign Keys: {fk}
- Column Types: {schema_types}

Based on the schema information, convert the SQL query to its corresponding natural language question (NLQ).
Only return a JSON object with a single key "nlq" containing your final NLQ.
"""
    }
]
        messages = [
    {
        "role": "system",
        "content": f"""SQL Query:
{sql}

Natural Language Question (NLQ):"""
    }
]

        nlq = make_request(messages, client, model_name)
        print("NLQ1",nlq)

        predicted_nlq = nlq
        print(predicted_nlq)

        print("nlq: ",predicted_nlq)
        
        new_row = pd.DataFrame({
            'Gold SQL': [row['query']],
            'Predicted NLQ': [predicted_nlq],
            'back_translation_NLQ': [row['question']],

            'Database': [row['db_id']],
            "hardness": [row['hardness']]
        })

        # Append the new row to the CSV file immediately.
        new_row.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)
        break
    
    # Close the progress bar when done.
    pbar.close()
