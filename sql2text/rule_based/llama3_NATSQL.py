from openai import OpenAI
from json_repair import repair_json
import pandas as pd
import glob
from tqdm import tqdm
from utils import  load_checkpoint, save_checkpoint
import time
import pandas as pd
from nltk.translate.bleu_score import corpus_bleu
from nltk.translate.bleu_score import sentence_bleu
from rouge import Rouge
import os
from tqdm import tqdm  # Import tqdm for progress tracking
# good_json_string = repair_json(bad_json_string)
# If the string was super broken this will return an empty string
def process( train_ds, dev_ds,client, model_name, checkpoint_path="outputs/llama3-NatSQL/checkpoint.pkl"):
    # Load the synthetic queries
    # Create a list to store the results
    results = []
    if checkpoint_path and os.path.exists(checkpoint_path):
        results = load_checkpoint(checkpoint_path)   
    messages=[
        {"role": "system", "content": "You are a database course instructor. You are helping a student convert SQL to NATSQL."},
        {"role": "user", "content":"""
    SELECT T1.title FROM film AS T1 JOIN film_actor AS T2 ON T1.film_id = T2.film_id GROUP
BY T1.film_id HAVING count(*) > 5 INTERSECT SELECT T1.title FROM film AS T1 JOIN
inventory AS T2 ON T1.film_id = T2.film_id GROUP BY T1.film_id HAVING count(*) < 3
    """},
        {"role": "assistant", "content": """{"NatSQL": "SELECT film.title WHERE count(film_actor.
*) > 5 and count(inventory.
*) < 3",
"""},
        {"role": "user", "content": """SELECT T1.name FROM student AS T1
JOIN has_pet AS T2 ON T1.stuid=T2.stuid"""} ,
        {"role": "assistant", "content": """{"NatSQL": "SELECT student.name FROM student, has_pet"}"""},
        {"role": "user", "content": """SELECT name FROM Properties WHERE code = "House" OR code = "Apartment" """} ,
        {"role": "assistant", "content": """{"NatSQL": "SELECT prop.name WHERE prop.code =
“House” OR prop.code = "Apartment" AND
prop.room > 1"}"""},
{"role": "user", "content": """SELECT count(*) FROM visitor WHERE
id NOT IN ( SELECT t2.visitor_id
FROM museum AS t1 JOIN visit AS
t2 ON t1.Museum_ID = t2.Museum_ID
WHERE t1.open_year > 2010 )"""},
{"role": "assistant", "content": """{"NatSQL": "SELECT count(visitor.*) WHERE @ NOT IN
visit.* and museum.open_year > 2010"}"""},
    ]

    # Iterate over the synthetic queries
    for i in tqdm(range(len(dev_ds)), desc="Processing queries"):
        # if dev_ds.iloc[i]["db_id"] != "concert_singer":
        #     continue

        prompt = {"role": "user", "content": f"""Convert SQL to NATSQL. The output should be a json format with just ONE key:'NatSQL'. Don't add any explanation. Here is the query: {dev_ds.iloc[i]["query"]}"""}
        messages.append(prompt)
        
        chat_completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            
            max_tokens=4096,
            extra_body={
                'stop_token_ids': [128009, 128001],
            }

        )
        bad_json_string = chat_completion.choices[0].message.content
        good_json_string = repair_json(bad_json_string,return_objects=True)
        natsql = good_json_string["NatSQL"]
        nat2nlq = [
            {"role": "system", "content": "You are a database course instructor. You are helping a student convert NATSQL to natural language question. the output should be a json format with just ONE key:'question'. Don't add explanation."},
            {"role": "user", "content": f"""SELECT student.name FROM student, has_pet"""},
            {"role": "assistant", "content": """{"question": "Find the name of students who have a pet"}"""},
            {"role": "user", "content": f"""{natsql}"""},

        ]
        chat_completion = client.chat.completions.create(
            model=model_name,
            messages=nat2nlq,
            
            max_tokens=4096,
            extra_body={
                'stop_token_ids': [128009, 128001],
            }

        )


        bad_json_string = chat_completion.choices[0].message.content
        good_json_string = repair_json(bad_json_string,return_objects=True)
        results.append({"query": dev_ds.iloc[i]["query"],"NatSQL": natsql, "question": good_json_string["question"], "original": dev_ds.iloc[i]["question"]})
        messages.pop()
        time.sleep(1)
        if (i + 1) % 10 == 0:
            save_checkpoint(results, checkpoint_path)
    # Save the results to csv
    results_df = pd.DataFrame(results)
    results_df.to_csv(f"outputs/llama3-NatSQL/res.csv", index=False)
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
    #     bleu_scores.append(bleu_score)

    # # Calculate corpus BLEU
    # corpus_bleu_score = corpus_bleu([[row['original'].split()] for index, row in df.iterrows()], 
    #                                  [row['question'].split() for index, row in df.iterrows()])

    # Calculate ROUGE scores
    rouge_scores = rouge.get_scores(df['question'], df['original'], avg=True)

    return rouge_scores

# Example usage:


if __name__ == "__main__":
    train_ds = pd.read_json("data/train_spider.json")
    dev_ds = pd.read_json("data/dev.json")
    j = 0
    client = OpenAI(
        api_key="EMPTY",
        base_url="http://anagram.cs.ualberta.ca:8000/v1",
)
    model = client.models.list().data[0].id
    print(model)
    # List of files to process
    process(train_ds, dev_ds, client, model,checkpoint_path = "outputs/llama3-NatSQL/checkpoint.pkl")
    csv_file = 'outputs/llama3-NatSQL/res.csv'  # Change this to your CSV file path
    rouge_scores = calculate_scores(csv_file)
    
    print("ROUGE Scores:", rouge_scores)
# save the evaluation results to a file
    with open("outputs/llama3-NatSQL/evaluation.txt", "w") as f:
        f.write(str(rouge_scores))
    
