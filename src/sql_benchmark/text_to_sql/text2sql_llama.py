import json
import os
import pickle
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm
from json_repair import repair_json
from sql_benchmark.query_generation.read_schema.read_schema import convert_json_to_schema
from openai import OpenAI

# Load environment variables
load_dotenv(find_dotenv())

# Initialize OpenAI client (in your case, it could be the LLaMA client)
client = OpenAI(
    api_key="token-wdmuofa",
    base_url="http://anagram.cs.ualberta.ca:2000/v1"
)
model = "meta-llama/Meta-Llama-3-70B-Instruct" # Choose one from the table

def predict_sql_with_llama(
    pbar, json_objects, dev, gold_file_name, predict_file_name, checkpoint_file
):
    checkpoint_interval = 5  # Set checkpoint interval

    gold_queries = []
    predicted_queries = []

    for i in range(len(json_objects), len(dev)):
        print(dev.iloc[i]["question"])
        query = dev.iloc[i]["query"]
        pbar.update(1)
        db_name = dev.iloc[i]["db_id"]
        schema_info = {}
        schema, pk, fk, schema_types = read_schema_pk_fk_types(
            db_name, "data/tables.json"
        )
        schema_info= {
            "schema": schema,
            "pk": pk,
            "fk": fk,
            "schema_types": schema_types,
        }

        # Make a single API request to LLaMA
        response = client.chat.completions.create(
            model=model,  # You can replace this with your LLaMA model name
            messages=[
                {
                    "role": "system",
                    "content": f"Write a query for the following question: {dev.iloc[i]['question']}. Here is the schema {schema_info['schema']}. Here are primary keys: {schema_info['pk']}, Here are foreign keys: {schema_info['fk']}. Here are column types: {schema_info['schema_types']}. The output should be in json format with 'predicted_sql' as a key."
                }
            ],
            temperature=0,
            max_tokens=1000
        )

        gold_queries.append(query)
        response_text = response.choices[0].message.content.strip()
        response_json = repair_json(response_text, return_objects=True)
        predicted_query = response_json["predicted_sql"]

        predicted_query = " ".join(predicted_query.split())
        predicted_queries.append(predicted_query)
        json_objects.append(predicted_query)

        if i % checkpoint_interval == 0:
            save_checkpoint_json(checkpoint_file, json_objects)

    # Save the final output to the output_file
    save_checkpoint_json(checkpoint_file, json_objects)

    with open(predict_file_name, "w") as f:
        f.write("\n".join(predicted_queries))

def run_task(filename, checkpoint_file):
    # db_name = (filename.split("/")[-1]).split(".")[0]
    gold_file_name = f"test-suite-sql-eval-master/evaluation_examples/llama3_simple_prompt/dev_gold.txt"
    predict_file_name = f"test-suite-sql-eval-master/evaluation_examples/llama3_simple_prompt/dev_predict.txt"

    dev = pd.read_json(filename)
    # print(questions)
    # return
    
    gold_queries = []
    string2write = ""
    for i in range(len(dev)):
        gold_queries.append((dev.iloc[i]["query"], dev.iloc[i]["db_id"]))
        string2write += dev.iloc[i]["query"] + "\t" + dev.iloc[i]["db_id"] + "\n"
    
    with open(gold_file_name, "w") as f:
        f.write(string2write)
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            json_objects = json.load(f)
    else:
        json_objects = []

    with tqdm(total=len(dev)-len(json_objects)) as pbar:
        predict_sql_with_llama(
            pbar,
            json_objects,
            dev,
            gold_file_name,
            predict_file_name,
            
            checkpoint_file
        )

        result = f"Task completed for file: {filename}"
    return result

def save_checkpoint_json(file_path, json_objects):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)

def read_schema_pk_fk_types(db_name, file_name, all_db=None):
    if all_db is None:
        all_db = convert_json_to_schema(file_name)
    schema = all_db[db_name]["schema"]
    pk = all_db[db_name]["primary_keys"]
    fk = all_db[db_name]["foreign_keys"]
    schema_types = all_db[db_name]["schema_types"]
    return schema, pk, fk, schema_types

# Main function to run the task on the single file 'data/dev.json'
if __name__ == "__main__":
    filename = "data/dev.json"
    checkpoint_path = "outputs/LLMS_predictions/llama3"
    checkpoint_file = f"{checkpoint_path}/checkpoint-dev.json"

    # Assuming the database name is 'dev'
    

    # Run the task on 'data/dev.json'
    result = run_task(filename, checkpoint_file)
    print(result)
