import glob
import json
import os
import pickle

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from langchain.chains import LLMChain

# use the load_metric function
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from tqdm import tqdm
from json_repair import repair_json
# from read_schema.read_schema import convert_json_to_schema

from query_generation.read_schema.read_schema import convert_json_to_schema

load_dotenv(find_dotenv()  )
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
from openai import OpenAI

client = OpenAI()


def predict_sql_with_GPT(
    pbar, json_objects, questions, gold_file_name, predict_file_name, schema_info, checkpoint_file
):
    checkpoint_interval = 5  # Set checkpoint interval

    print(questions)
    gold_queries = []
    predicted_queries = []

    for i in range(len(json_objects), len(questions)):
        print(questions.iloc[i]["question"])
        query = questions.iloc[i]["query"]
        pbar.update(1)

        # Make a single API request to OpenAI
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"Write a query for the following question: {questions.iloc[i]['question']}. Here is the schema {schema_info['schema']}. Here are primary keys: {schema_info['pk']}, Here are foreign keys: {schema_info['fk']}. Here are column types: {schema_info['schema_types']}. The output should be in json format with 'predicted_sql' as a key."
                }
            ],
            temperature=0,
            max_tokens=1000
        )

        gold_queries.append(query)
        print(response)
        response_text = response.choices[0].message.content.strip()
        response_json = repair_json(response_text, return_objects=True)
        predicted_query = response_json["predicted_sql"]

        print(predicted_query)
        predicted_query = " ".join(predicted_query.split())
        predicted_queries.append(predicted_query)
        json_objects.append(predicted_query)

        if i % checkpoint_interval == 0:
            print("__________________________")
            save_checkpoint_json(checkpoint_file, json_objects)

    # Save the final output to the output_file
    save_checkpoint_json(checkpoint_file, json_objects)

    with open(predict_file_name, "w") as f:
        f.write("\n".join(predicted_queries))

def run_task(filename, checkpoint_file, schema_info):
    db_name = (filename.split("/")[-1]).split(".")[0]
    gold_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/gpt-3.5-turbo/{db_name}_gold.txt"
    )
    predict_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/gpt-3.5-turbo/{db_name}_predict.txt"
    )
    print(filename)
    questions = pd.read_csv(filename)
    gold_queries = []
    for i in range(len(questions)):
        gold_queries.append(questions.iloc[i]["query"])
    with open(gold_file_name, "w") as f:
        string2write = f"\t{db_name}\n".join(gold_queries) + f"\t{db_name}"
        f.write(string2write)
    if os.path.exists(checkpoint_file):
        print("Checkpoint file exists")
        with open(checkpoint_file, "r") as f:
            json_objects = json.load(f)
    else:
        json_objects = []

    with tqdm(total=len(questions)-len(json_objects)) as pbar:
        predict_sql_with_GPT(
            pbar,
            json_objects,
            questions,
            gold_file_name,
            predict_file_name,
            schema_info,
            checkpoint_file
        )

        result = f"Task completed for file: {filename}"
    return result

def run_task_GPT(filename, checkpoint_path, schema_info):
    print("HIII")
    db_name = (filename.split("/")[-1]).split(".")[0]
    checkpoint_file = f"{checkpoint_path}/checkpoint-{db_name}.json"
    print(checkpoint_file)
    print(filename)
    result = run_task(
        filename, checkpoint_file, schema_info
    )
    return result

# If you want to run the script directly, uncomment the following lines:
# folder_path = "outputs"
# files_to_process = glob.glob(folder_path + "/*.json")
# print(files_to_process)
# files_to_process = ["outputs/checkpoint-farm.json"]
# schema_info = {}
# for file in files_to_process:
#     db_name = file.split(".")[0].split("-")[1]
#     schema, pk, fk, schema_types = read_schema_pk_fk_types(
#         db_name, "test-suite-sql-eval-master/tables.json"
#     )
#     schema_info[db_name] = {
#         "schema": schema,
#         "pk": pk,
#         "fk": fk,
#         "schema_types": schema_types,
#     }

# last_processed_file = load_checkpoint(llm="gpt-3.5-turbo")
# if last_processed_file:
#     last_processed_index = files_to_process.index(last_processed_file)
#     files_to_process = files_to_process[last_processed_index + 1:]
# print("files", files_to_process)
# for filename in tqdm(files_to_process):
#     db_name = file.split(".")[0].split("-")[1]
#     result = run_task(
#         filename, checkpoint_path, schema_info[db_name], llm="gpt-3.5-turbo"
#     )
#     save_checkpoint(filename, llm="gpt-3.5-turbo")
#     tqdm.write(result)


def save_checkpoint_json(file_path, json_objects):
    print("Saving checkpoint")
    print(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)


def save_checkpoint_text(file_path, json_objects):
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)


def save_checkpoint(filename, llm):
    # split the filename to get the name of the file
    with open(f"outputs/LLMS_predictions/{llm}/checkpoint.pkl", "wb") as f:
        pickle.dump(filename, f)


def load_checkpoint(llm):
    try:
        with open(f"outputs/LLMS_predictions/{llm}/checkpoint.pkl", "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None




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




# if __name__ == "__main__":

# folder_path = "outputs"

# # Use glob to find files in the folder
# files_to_process = glob.glob(folder_path + "/*.json")
# print(files_to_process)
# files_to_process = ["outputs/checkpoint-farm.json"]
# schema_info = {}
# for file in files_to_process:
#     db_name = file.split(".")[0].split("-")[1]

#     schema, pk, fk, schema_types = read_schema_pk_fk_types(
#         db_name, "test-suite-sql-eval-master/tables.json"
#     )
#     schema_info[db_name] = {
#         "schema": schema,
#         "pk": pk,
#         "fk": fk,
#         "schema_types": schema_types,
#     }

# # Load checkpoint if available
# last_processed_file = load_checkpoint(llm="gpt-3.5-turbo")

# if last_processed_file:
#     # Find the index of the last processed file
#     last_processed_index = files_to_process.index(last_processed_file)
#     files_to_process = files_to_process[last_processed_index + 1 :]
# print("files", files_to_process)
# print(tqdm(files_to_process))
# for filename in tqdm(files_to_process):
#     # filename = filename.split("/")[-1].split("_")[0]
#     db_name = file.split(".")[0].split("-")[1]

#     result = run_task(
#         filename, checkpoint_path, schema_info[db_name], llm="gpt-3.5-turbo"
#     )
#     # Save checkpoint after processing each file
#     save_checkpoint(filename, llm="gpt-3.5-turbo")
#     tqdm.write(result)