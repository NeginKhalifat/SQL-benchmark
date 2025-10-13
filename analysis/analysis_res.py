import ast
import json
import os
import subprocess

import pandas as pd
from tqdm import tqdm


def get_accuracy():
    llms = ["gpt-3.5-turbo"]
    # llms = ["gemini-pro"]
    for llm in llms:
        print(llm)
        dbs = []

        for file in os.listdir(f"test-suite-sql-eval-master/evaluation_examples/{llm}"):
            
            
           
            # print(file)
            # print(file.split("_")[0])
            if file.split("_")[0] not in dbs and not file.endswith("_predict.txt"):
                dbs.append(file.split("_gold.txt")[0])
                
                # print(file)
                
                # print(client.models.list().data)



                # model= "meta-llama/Meta-Llama-3-8B-Instruct" # Choose one from the table
                # print(model)
                # List of files to process
                # process( data, client, model,checkpoint_path = "outputs/llama3/synthetic_data/checkpoint.pkl", db_name =db_name,schema= schema,all_db=all_db)
                #     csv_file = 'outputs/llama3/synthetic_data/res.csv'  # Change this to your CSV file path
                # rouge_scores = calculate_scores(csv_file)

                # print("ROUGE Scores:", rouge_scores)
                # # save the evaluation results to a file
                # with open("outputs/llama3/synthetic_data/evaluation.txt", "w") as f:
                #     f.write(str(rouge_scores))
                #
            # print(file)
        print("dbs:", dbs)
        
            
        for db in dbs:
            print(db)
            with open(f"outputs/analysis/{llm}_{db}.txt", "w") as output_file:
    # Run the command and redirect both stdout and stderr to the file
            # process = subprocess.run(command, shell=True, stdout=output_file, stderr=subprocess.STDOUT)
                cmd_str = f"""python3 test-suite-sql-eval-master/evaluation.py --gold test-suite-sql-eval-master/evaluation_examples/llm_based_gold.txt --pred test-suite-sql-eval-master/evaluation_examples/llm_based_gpt_4_turbo_res.txt --db test-suite-sql-eval-master/database/ --etype all --table test-suite-sql-eval-master/tables.json """
                print(cmd_str)
                subprocess.run(cmd_str, shell=True,    stdout=output_file, stderr=subprocess.STDOUT)

                # print(result.stdout)
            
        # print("##########################")


if __name__ == "__main__":
    get_accuracy()
    # print(acc)