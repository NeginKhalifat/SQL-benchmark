from openai import OpenAI


from json_repair import repair_json
import pandas as pd
from tqdm import tqdm
from utils import  load_checkpoint, save_checkpoint
import time
import pandas as pd
from rouge import Rouge
import os
from together import Together
from tqdm import tqdm  # Import tqdm for progress tracking

# good_json_string = repair_json(bad_json_string)
# If the string was super broken this will return an empty string
def process( train_ds, dev_ds,client, model_name,checkpoint_path="outputs/llama2/checkpoint.pkl"):
    # Load the synthetic queries
    # Create a list to store the results
    results = []
    if checkpoint_path and os.path.exists(checkpoint_path):
        results = load_checkpoint(checkpoint_path)

    messages=[
        {"role": "system", "content": "You are a database course instructor. You are helping a student convert SQL to natural language question."},
        {"role": "user", "content": "SELECT Official_Name ,  Status FROM city ORDER BY Population DESC LIMIT 1"},
        {"role": "assistant", "content": """{"question": "List the official name and status of the city with the largest population."}"""},
        {"role": "user", "content": "SELECT T2.Year ,  T1.Official_Name FROM city AS T1 JOIN farm_competition AS T2 ON T1.City_ID  =  T2.Host_city_ID"} ,
        {"role": "assistant", "content": """{"question": "Show the years and the official names of the host cities of competitions."}"""},

    ]
    # Iterate over the synthetic queries
    for i in tqdm(range(len(dev_ds)), desc="Processing queries"):
        # if dev_ds.iloc[i]["db_id"] != "concert_singer":
        #     continue

        prompt = {"role": "user", "content": f"""Convert SQL to natural question. The output should be a json format with just ONE key:'question' here is the query: {dev_ds.iloc[i]["query"]}"""}
        messages.append(prompt)
        
        chat_completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
        )
        bad_json_string = chat_completion.choices[0].message.content
        good_json_string = repair_json(bad_json_string,return_objects=True)
        results.append({"query": dev_ds.iloc[i]["query"], "question": good_json_string["question"], "original": dev_ds.iloc[i]["question"]})
        messages.pop()
        time.sleep(1)
        if (i + 1) % 10 == 0:
            save_checkpoint(results, checkpoint_path)
    # Save the results to csv
    results_df = pd.DataFrame(results)
    results_df.to_csv(f"outputs/llama2/res.csv", index=False)
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
    client = Together(api_key=os.environ.get("TOGETHER_API_KEY"))
    model = "togethercomputer/llama-2-7b-chat"
    print(model)
    # List of files to process
    process(train_ds, dev_ds, client, model, checkpoint_path="outputs/llama2/checkpoint.pkl")
    csv_file = 'outputs/llama2/res.csv'  # Change this to your CSV file path
    rouge_scores = calculate_scores(csv_file)
    print("ROUGE Scores:", rouge_scores)
    # save the evaluation results to a file
    with open("outputs/llama2/evaluation.txt", "w") as f:
        f.write(str(rouge_scores))
        
    



#add checkpoints and taqadm to this file to store the updates and the results , rewrite the file
