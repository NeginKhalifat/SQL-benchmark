

from openai import OpenAI
from json_repair import repair_json
import pandas as pd
import glob
from utils import  load_checkpoint, save_checkpoint
import time
import pandas as pd
from nltk.translate.bleu_score import corpus_bleu
from nltk.translate.bleu_score import sentence_bleu
from rouge import Rouge
import os
from tqdm import tqdm  # Import tqdm for progress tracking
from query_generation.read_schema.read_schema import read_schema_pk_fk_types, convert_json_to_schema
import argparse


# Argument parser
parser = argparse.ArgumentParser(description='')
parser.add_argument("--synthesis_method", help="Method to synthesize SQL queries", type=str, choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement'], default='schema_guided')
args = parser.parse_args()


def make_request(messages,client, model_name):
    response = client.chat.completions.create(
        model=model_name,
        messages=messages,
        max_tokens=512,
 
    )
    good_json_string = repair_json(response.choices[0].message.content,return_objects=True)
  

    return good_json_string
# good_json_string = repair_json(bad_json_string)
# If the string was super broken this will return an empty string
def process(  dev_ds,client, model_name, checkpoint_path, db_name, schema,all_db, output_folder):
    # Load the synthetic queries
    # Create a list to store the results
    start_index = 0
    print(checkpoint_path)
    results = []
    if checkpoint_path and os.path.exists(checkpoint_path):
        results = load_checkpoint(checkpoint_path)  
        start_index = len(results) 
        print("Loaded checkpoint")
    sql2text_messages = [
        {"role": "system", "content": "You are a database course instructor. You are helping a student convert SQL to a natural language question. Given a SQL query, schema, and hint, convert it to a natural language question. The output should be in JSON format with a key 'question'."},
        {"role": "user", "content": f"""{{"query": "SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'","schema":{all_db["department_management"]},  "hint": "Focus on the DISTINCT clause and the JOIN conditions."}}"""},
        {"role": "assistant", "content": """{"question": "What are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?"}"""},
        {"role": "user", "content": f"""{{"query": "SELECT count(*) FROM department WHERE department_id NOT IN (SELECT department_id FROM management);","schema": {all_db["department_management"]} ,"hint": "Consider the COUNT function and the subquery used with NOT IN."}}"""},
        {"role": "assistant", "content": """{"question": "How many departments are led by heads who are not mentioned?"}"""}
    ]
    # self_consistency_messages= [
    #     {"role": "system", "content": "Imagine 3 completely independent database course instructors who reason differently helping a student convert SQL to a natural language question. Given a SQL query, schema, convert it to a natural language question. The final answe obtained by majority vote. The output should be in JSON format with a key 'question'."},
    #     {"role": "user", "content": f"""{{"query": "SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'","schema":{all_db["department_management"]}}}"""},
    #     {"role": "assistant", "content": """{"question": "What are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?"}"""},
    #     {"role": "user", "content": f"""{{"query": "SELECT count(*) FROM department WHERE department_id NOT IN (SELECT department_id FROM management);","schema": {all_db["department_management"]} }}"""},
    #     {"role": "assistant", "content": """{"question": "How many departments are led by heads who are not mentioned?"}"""}
  
    # ]

   
    end = len(dev_ds)
    if len(dev_ds)>25:
        end = 25
    print("Starting from index", start_index)
    # Iterate over the synthetic queries
    for i in tqdm(range(start_index, end), desc="Processing queries"):
        print(dev_ds.iloc[i]["sql"])
        if not dev_ds.iloc[i]["sql"].startswith("SELECT"):
            continue
        # if dev_ds.iloc[i]["db_id"] != "concert_singer":
        #     continue
        flag = False
        hint = ""
        max_iter = 5
        final_question = ""
        while not flag and max_iter > 0:
            print("iter",5-max_iter+1) 
            sql2text_messages = [
        {"role": "system", "content": "You are a database course instructor. You are helping a student convert SQL to a natural language question. Given a SQL query, schema, and hint, convert it to a natural language question. The output should be in JSON format with a key 'question'."},
        {"role": "user", "content": f"""{{"query": "SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'","schema":{all_db["department_management"]},  "hint": "Focus on the DISTINCT clause and the JOIN conditions."}}"""},
        {"role": "assistant", "content": """{"question": "What are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?"}"""},
        {"role": "user", "content": f"""{{"query": "SELECT count(*) FROM department WHERE department_id NOT IN (SELECT department_id FROM management);","schema": {all_db["department_management"]} ,"hint": "Consider the COUNT function and the subquery used with NOT IN."}}"""},
        {"role": "assistant", "content": """{"question": "How many departments are led by heads who are not mentioned?"}"""}
    ]
            prompt = {"role": "user", "content": f"""Convert SQL to natural question. The output should be a json format with just ONE key:'question' Dont explain anything. \n {{ "query": {dev_ds.iloc[i]["sql"]}, "schema":{schema}, "hint":{hint}}}"""}
            sql2text_messages.append(prompt)
            try:
                
                text=make_request(sql2text_messages,client,model_name)
            except:
                # print("Hi")
                # print(all_db[db_name]["schema"])
                sql2text_messages.pop()
                prompt = {"role": "user", "content": f"""Convert SQL to natural question. The output should be a json format with just ONE key:'question' Dont explain anything. \n {{ "query": {dev_ds.iloc[i]["sql"]}, "schema":{all_db[db_name]['schema']},"fk":{all_db[db_name]['foreign_keys']}, "hint":{hint}}}"""}
                sql2text_messages.append(prompt)
                # print(sql2text_messages)
                text = make_request(sql2text_messages,client,model_name)
            print(text)
            print("************************")
            final_question = text["question"]
        # print(final_question)

            print("-------------------")
            # print(dev_ds.iloc[i]["query"])
            print(text["question"])
            # convert the question to a SQL query
            text2sql_messages=[
        {"role": "system", "content": "You are a database course instructor. You are helping a student convert text to SQL query. Given a natural language question and schema, convert it to a sql query."},
        {"role": "user", "content": f"""{{"question": "What are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?", "schema": {all_db["department_management"]}}}"""},
        {"role": "assistant", "content": f"""{{"query": "SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'"}}"""}
    ]
            prompt = {"role": "user", "content": f"""Convert natural question to SQL query. The output should be a json format with just ONE key:'query' Dont explain anything. {{ "question": {text["question"]},"schema":{schema}}}"""}
            text2sql_messages.append(prompt)
            try:
                good_json_string = make_request(text2sql_messages,client,model_name)
            except:
                text2sql_messages.pop()
                prompt = {"role": "user", "content": f"""Convert natural question to SQL query. The output should be a json format with just ONE key:'query' Dont explain anything. {{ "question": {text["question"]},"schema":{all_db[db_name]['schema']},"fk":{all_db[db_name]['foreign_keys']}}}"""}
                text2sql_messages.append(prompt)
                good_json_string = make_request(text2sql_messages,client,model_name)

            print(good_json_string["query"])
            print("-------------------")
            # Compare the two SQL queries
            compare_sql_messages = [
    {"role": "system", "content":  (
                        "You are an SQL expert. Your task is to determine whether two SQL queries produce the same results given the same database schema. "
                        "Analyze the two queries and return a JSON object with the following keys:\n"
                        "  - 'result': a boolean indicating whether the queries are equivalent.\n"
                        "  - 'hint': a general hint to improve the natural language question if 'result' is false.\n"
                        "The hint should be general and not reference specific queries, table names, or column names. "
                        "It should guide the user on how to improve their question for better SQL translation."
                    )},
    {"role": "user", "content": f"""
        "query1": "SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'", 
        "query2": "SELECT count(*) FROM department WHERE department_id NOT IN (SELECT department_id FROM management);", 
        "schema": {all_db["department_management"]}
    """},
    {"role": "assistant", "content": """
        {"result": false, "hint": "Please pay more attention to the SELECT clause and the use of joins."}
    """},
    {"role": "user", "content": f"""
        "query1": "SELECT T1.department_id FROM department AS T1 WHERE T1.creation > '2000-01-01'", 
        "query2": "SELECT department_id FROM department WHERE creation > '2000-01-01'", 
        "schema": {all_db["department_management"]}
    """},
    {"role": "assistant", "content": """
        {"result": true, "hint": ""}
    """}
]
            prompt = {"role": "user", "content": f"""{{"query1": {dev_ds.iloc[i]["sql"]}, "query2": {good_json_string["query"]}, "schema": {schema}}}"""}
            compare_sql_messages.append(prompt)
            # print(compare_sql_messages)
            try:
                
                result = make_request(compare_sql_messages,client,model_name)
            except:
                compare_sql_messages.pop()
                prompt = {"role": "user", "content": f"""{{"query1": {dev_ds.iloc[i]["sql"]}, "query2": {good_json_string["query"]},  "schema":{all_db[db_name]['schema']},"fk":{all_db[db_name]['foreign_keys']}"""}
                compare_sql_messages.append(prompt)
                result = make_request(compare_sql_messages,client,model_name)


            print("RESULT",result["result"])
            flag = result["result"]
            hint = hint +"\n"+result["hint"]
            print("HINT",hint)
            max_iter -= 1
            

        


        results.append({"query": dev_ds.iloc[i]["sql"], "question": final_question, "iter":5-max_iter+1, "flag": flag})
        time.sleep(1)
        if (i + 1) % 2 == 0:
            save_checkpoint(results, checkpoint_path)
    # Save the results to csv
    results_df = pd.DataFrame(results)
    results_df.to_csv(f"{output_folder}/{db_name}.csv", index=False)
    return 



def calculate_scores(csv_file):
    # Load the CSV file
    df = pd.read_csv(csv_file)

    # Initialize BLEU and ROUGE scorers
    rouge = Rouge()
    bleu_scores = []

    # Calculate scores for each row
    # for index, row in df.iterrows():
    #     pred_question = row['question']
    #     original_question = row['original']

    #     # Calculate BLEU score
    #     bleu_score = sentence_bleu([original_question.split()], pred_question.split())
    #     print(bleu_score)
    #     bleu_scores.append(bleu_score)

    # # Calculate corpus BLEU
    # corpus_bleu_score = corpus_bleu([[row['original'].split()] for index, row in df.iterrows()], 
    #                                  [row['question'].split() for index, row in df.iterrows()])

    # Calculate ROUGE scores
    rouge_scores = rouge.get_scores(df['question'], df['original'], avg=True)

    return rouge_scores

# Example usage:


if __name__ == "__main__":
    folder_path = f"data/synthetic-queries/{args.synthesis_method}/correct_sqls"
    OUTPUT_FOLDER=f"outputs/experiments/SQL2text/{args.synthesis_method}"
    checkpoint_folder = f"{OUTPUT_FOLDER}/checkpoints/"
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(checkpoint_folder,exist_ok=True)

    from openai import OpenAI

    client = OpenAI(
        api_key="token-wdmuofa",
        base_url="http://anagram.cs.ualberta.ca:2000/v1" # Choose one from the table
        # base_url = "http://turin4.cs.ualberta.ca:2001/v1"
        
    )
     
    files_to_process = glob.glob(folder_path + "/*.csv")
    # model = "meta-llama/Meta-Llama-3-70B-Instruct" # Choose one from the table
    processed_files = glob.glob(OUTPUT_FOLDER + "/*.csv")
    # print(processed_files)


    
    
    all_db = convert_json_to_schema("data/tables.json", col_exp=True)

    # # print(chat_completion.choices[-1].message.content)
    for file in files_to_process:
        db_name = file.split("/")[-1].split(".")[0].split("_correct_sqls")[0]
        if f"{OUTPUT_FOLDER}/{db_name}.csv" in processed_files:
            print("Already Processed")
            continue
        print(db_name)
        schema, pk, fk, schema_types, schema_desc = read_schema_pk_fk_types(db_name, "data/tables.json", all_db=all_db,col_exp = True)
        
        data = pd.read_csv(file)
        print(len(data))
        j = 0
        print("Starting processing")
        # print(client.models.list().data)

        model = "meta-llama/Meta-Llama-3.1-70B-Instruct"

        print(model)
        # List of files to process
        process( data, client, model,checkpoint_path = f"{OUTPUT_FOLDER}/checkpoints/{db_name}_checkpoint.pkl", db_name =db_name,schema= schema_desc,all_db=all_db,output_folder=OUTPUT_FOLDER)
    #     csv_file = 'outputs/llama3/synthetic_data/res.csv'  # Change this to your CSV file path
    # # rouge_scores = calculate_scores(csv_file)
    
    # # print("ROUGE Scores:", rouge_scores)
    # # # save the evaluation results to a file
    # # with open("outputs/llama3/synthetic_data/evaluation.txt", "w") as f:
    # #     f.write(str(rouge_scores))
    