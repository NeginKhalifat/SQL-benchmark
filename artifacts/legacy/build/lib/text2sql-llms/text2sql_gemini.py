import json
import os
import pickle

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm
from json_repair import repair_json
import time
# from sql2text.rule_based.utils import string_to_dict

load_dotenv(find_dotenv())
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

genai.configure(api_key=GOOGLE_API_KEY)


def predict_sql_with_GEMINI(
    pbar, json_objects, model, questions, gold_file_name, predict_file_name, schema_info, checkpoint_file
):
    checkpoint_interval = 5  # Set checkpoint interval
    # checkpoint_file = predict_file_name
    print(questions)
    gold_queries = []
    predicted_queries = []
    print(len(questions))
    print(len(json_objects))
    

    for i in range(len(json_objects), len(questions)):
        print(questions.iloc[i]["question"])
        query = questions.iloc[i]["query"]
        pbar.update(1)
        template = (
            f"""Write a query for the folowing question:{questions.iloc[i]["question"]}. Here is the schema {schema_info["schema"]}. Here are primary keys: {schema_info["pk"]}, Here are foreign keys: {schema_info["fk"]}.  Here are column types: {schema_info["schema_types"]} \nThe output should be in json format with 'predicted_sql' as a key don't use extra characters like \n or \t or `""",
        )

        response = model.generate_content(template).text
        time.sleep(4)
        gold_queries.append(query)
        response =repair_json(response, return_objects=True)
        # response = string_to_dict(response, partial=True)
        print(response)

        predicted_query = response["predicted_sql"]
        print(predicted_query)
        predicted_query = predicted_query.replace("\\n", " ").replace("`", " ").replace("\\\"", "\"")
        predicted_query = " ".join(predicted_query.split())
        predicted_queries.append(predicted_query)
        json_objects.append(predicted_query)

        if i % checkpoint_interval == 0:
            print("HIiiiiiiiiiiiiii__________________-")
            save_checkpoint_json(checkpoint_file, json_objects)

    # Save the final output to the checkpoint_file
    save_checkpoint_json(checkpoint_file, json_objects)

    # Open the predict_file_name in write mode
    with open(predict_file_name, "w") as f:
        # Write the list of predicted_queries into the file
        f.write("\n".join(json_objects))

    # Open the gold_file_name in write mode
  


def save_checkpoint_json(file_path, json_objects):

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    print(file_path)
    with open(file_path, "w") as f:
        json.dump(json_objects, f, indent=4)


def run_task(filename, checkpoint_path, model, schema_info, llm):
    print("HIII")
    print((filename.split("/")[-1]).split(".")[0])
    db_name = (filename.split("/")[-1]).split(".")[0]
    gold_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/{llm}/{db_name}_gold.txt"
    )
    predict_file_name = (
        f"test-suite-sql-eval-master/evaluation_examples/{llm}/{db_name}_predict.txt"
    )
    print(filename)
    questions = pd.read_csv(filename)
    gold_queries = []
    for i in range(len(questions)):
        gold_queries.append( questions.iloc[i]["query"])
    with open(gold_file_name, "w") as f:
        # Write the list of JSON objects into the file
        string2write = f"\t{db_name}\n".join(gold_queries) + f"\t{db_name}"
        f.write(string2write)
    checkpoint_file = f"{checkpoint_path}/checkpoint-{db_name}.json"
    print(checkpoint_file)
    if os.path.exists(checkpoint_file):
        print("Checkpoint file exists")
        with open(checkpoint_file, "r") as f:
            json_objects = json.load(f)
    else:
        json_objects = []

    with tqdm(total=len(questions)- len(json_objects)) as pbar:
        predict_sql_with_GEMINI(
            pbar,
            json_objects,
            model,
            questions,
            gold_file_name,
            predict_file_name,
            schema_info,
            checkpoint_file
        )

        result = f"Task completed for file: {filename}"
    return result


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


def run_task_Gemini(filename, checkpoint_path, schema_info):
    # Set up the model
    generation_config = {
        "temperature": 0,
        "top_p": 1,
        "top_k": 1,
        "max_output_tokens": 400,
    }

    safety_settings = [
        {
            "category": "HARM_CATEGORY_HARASSMENT",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_HATE_SPEECH",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
            "threshold": "BLOCK_NONE"
        }
    ]

    model = genai.GenerativeModel(model_name="gemini-pro",
                              generation_config=generation_config,
                              safety_settings=safety_settings)
    result = run_task(filename, checkpoint_path, model, schema_info, llm="gemini-pro")
    return result


if __name__ == "__main__":
    model = genai.GenerativeModel("gemini-pro")
    response = model.generate_content("What is the meaning of life?")
    print(response.text)
