import glob
import json
import os
import pickle

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from text2sql_gemini import run_task_Gemini
from text2sql_gpt import run_task_GPT
from tqdm import tqdm

# from helper.read_schema import convert_json_to_schema
from query_generation.read_schema.read_schema import convert_json_to_schema
load_dotenv(find_dotenv())
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

genai.configure(api_key=GOOGLE_API_KEY)


# print(HUMAN_SQL2TEXT)


def predict_sql_with_GPT(
    pbar, json_objects, chain, questions, gold_file_name, predict_file_name, schema_info
):
    checkpoint_interval = 5  # Set checkpoint interval
    checkpoint_file = predict_file_name
    print(questions)
    gold_queries = []
    predicted_queries = []

    for i in range(len(json_objects), len(questions)):
        print(questions.iloc[i]["corrected_text"])
        query = questions.iloc[i]["query"]
        pbar.update(1)
        response = chain(
            {
                "question": questions.iloc[i]["corrected_text"],
                "schema": schema_info["schema"],
                "pk": schema_info["pk"],
                "fk": schema_info["fk"],
                "schema_types": schema_info["schema_types"],
            }
        )
        gold_queries.append(query)
        print(response)
        response = response["text"]
        response = response.replace("\t", "")
        response = response.replace("\n", "")
        response = response.replace(",}", "}")
        response = response.replace(",]", "]")
        predicted_query = json.loads(response)["predicted_sql"]
        print(predicted_query)
        predicted_query = " ".join(predicted_query.split())
        predicted_queries.append(predicted_query)

        if i % checkpoint_interval == 0:
            save_checkpoint_json(checkpoint_file, json_objects)

        # Save the final output to the output_file
        save_checkpoint_json(checkpoint_file, json_objects)

    # Open the file in append mode
    with open(predict_file_name, "w") as f:
        # Write the list of JSON objects into the file
        f.write("\n".join(json_objects))
    # with open(gold_file_name, "w") as f:
    #     # Write the list of JSON objects into the file
    #     string2write = "\tfarm\n".join(gold_queries) + "\tfarm"
    #     f.write(string2write)


def save_checkpoint_json(file_path, json_objects):
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)


def run_task(filename, checkpoint_path, chain, schema_info):
    print("HIII")
    print((filename.split("/")[-1]).split(".")[0].split("-")[1])
    db_name = (filename.split("/")[-1]).split(".")[0].split("-")[1]
    gold_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/{db_name}_gold.txt"
    )
    predict_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/{db_name}_predict.txt"
    )
    # write all the query in goldfile
    gold_queries = []
    for i in range(len(questions)):
        gold_queries.append( questions.iloc[i]["query"])
    with open(gold_file_name, "w") as f:
        # Write the list of JSON objects into the file
        string2write = f"\t{db_name}\n".join(gold_queries) + "\t{db_name}"
        f.write(string2write)
    print(filename)
    questions = pd.read_json(filename)

    checkpoint_file = f"{checkpoint_path}/checkpoint-{db_name}.json"
    if os.path.exists(checkpoint_file):
        print("Checkpoint file exists")
        with open(checkpoint_file, "r") as f:
            json_objects = json.load(f)
    else:
        json_objects = []

    with tqdm(total=len(questions)) as pbar:
        predict_sql_with_GPT(
            pbar,
            json_objects,
            chain,
            questions,
            gold_file_name,
            predict_file_name,
            schema_info,
        )

        result = f"Task completed for file: {filename}"
    return result


def save_checkpoint_text(file_path, json_objects):
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)


def save_checkpoint(filename, llm):
    # split the filename to get the name of the file
    os.makedirs(f"outputs/LLMS_predictions/{llm}", exist_ok=True)
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


if __name__ == "__main__":
    # llms = ["gemini-pro"]
    # llms = ["gemini-pro"]
    llms = ["gpt-3.5-turbo","gemini-pro"]
    for llm in llms:
        folder_path = "outputs/llama3/synthetic_data/llm_generated"

        # Use glob to find files in the folder
        files_to_process = glob.glob(folder_path + "/*.csv")
        print(files_to_process)
        # files_to_process = ["outputs/llama3/synthetic_data/farm.csv"]
        schema_info = {}
        for file in files_to_process:
            db_name = file.split(".")[0].split("/")[-1]
            print(db_name)

            schema, pk, fk, schema_types = read_schema_pk_fk_types(
                db_name, "test-suite-sql-eval-master/tables.json"
            )
            schema_info[db_name] = {
                "schema": schema,
                "pk": pk,
                "fk": fk,
                "schema_types": schema_types,
            }

        checkpoint_path = f"outputs/LLMS_predictions/{llm}"

        # Load checkpoint if available
        last_processed_file = load_checkpoint(llm)

        if last_processed_file:
            # Find the index of the last processed file
            last_processed_index = files_to_process.index(last_processed_file)
            files_to_process = files_to_process[last_processed_index + 1 :]
        print("files", files_to_process)
        print(tqdm(files_to_process))
        if llm == "gpt-3.5-turbo":
            for filename in tqdm(files_to_process):
                # filename = filename.split("/")[-1].split("_")[0]
                db_name = filename.split(".")[0].split("/")[-1]
                
                result = run_task_GPT(filename, checkpoint_path, schema_info[db_name])

                # Save checkpoint after processing each file
                save_checkpoint(filename, llm)
                tqdm.write(result)
        elif llm == "gemini-pro":
            for filename in tqdm(files_to_process):
                # filename = filename.split("/")[-1].split("_")[0]
                db_name = filename.split(".")[0].split("/")[-1]
                print(db_name)

                result = run_task_Gemini(
                    filename, checkpoint_path, schema_info[db_name]
                )
                # Save checkpoint after processing each file
                save_checkpoint(filename, llm)
                tqdm.write(result)



