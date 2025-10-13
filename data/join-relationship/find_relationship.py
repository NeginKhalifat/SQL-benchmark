import json
from openai import OpenAI
from json_repair import repair_json
import time
from query_generation.read_schema import convert_json_to_schema
import os

from itertools import permutations
from tqdm import tqdm

def get_table_combinations(json_file):
    table_tuples = []
    dict_tuples={}
    # Read the JSON file
    with open(json_file, 'r') as file:
        data = json.load(file)
    for row in data:
        
        table_names = row['table_names_original']
    
        # Generate combinations of table names
        combinations_list = list(permutations(table_names, 2))
        table_tuples.append({row['db_id']: combinations_list})
        dict_tuples[row['db_id']]=combinations_list
        

    with open('data/join-relationship/table_tuples.json', 'w') as file:
        json.dump(table_tuples, file, indent=4)
    return combinations_list,dict_tuples

# Example usage:
json_file = 'data/tables.json'  # Replace this with your JSON file path
combinations,dict_tuples = get_table_combinations(json_file)
def find_relationship():
    client = OpenAI(
        api_key="EMPTY",
        base_url="http://anagram.cs.ualberta.ca:8000/v1",
    )
    
    model_name = client.models.list().data[0].id
    schema_info = convert_json_to_schema("data/tables.json")
    all_combinations = dict_tuples
    all_relations = dict()
    messages = [
        {
            "role": "system",
            "content": "Identify verbs that describe the relationships between pairs of entities in a database schema. If there is an indirect relationship, infer that. First, we provide schema info, followed by one pair of entities. Your task is to find the appropriate verb that describes the relationship between each pair. The output format should be in JSON format with the key 'rel' and value as a triplet [entity1, entity2, verb]. Don't add any explanation."
        }
    ]
    
    # Checkpoint 1: Load progress from file if it exists
    checkpoint_file = "data/join-relationship/checkpoint.pkl"
    flag = False
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            checkpoint = json.load(f)
            db_id_start = checkpoint["db_id"]
            pair_index = checkpoint["pair_index"]
    else:
        db_id_start = "farm"
        pair_index = 0
    for db_id, combinations_list in tqdm(all_combinations.items(), desc="Processing"):
        if  flag == False and db_id != db_id_start:
            continue

        if db_id == db_id_start:
            pair_range = list(range(pair_index, len(combinations_list)))
            flag = True
        else:
            pair_range = range(len(combinations_list))
           
        for pair_idx in tqdm(pair_range, desc=f"DB ID: {db_id}"):
            pair = all_combinations[db_id][pair_idx]
            messages.append(
                {
                    "role": "user",
                    "content": f"schema_info: {schema_info[db_id]}\nPairs: {pair}\n",
                }
            )
            
            chat_completion = client.chat.completions.create(
                model=model_name,
                messages=messages,
                max_tokens=4096,
                extra_body={
                    "stop_token_ids": [128009, 128001],
                }
            )
            
            time.sleep(1)
            messages.pop()
            print("-------------------")
            bad_json_string = chat_completion.choices[0].message.content
            print(bad_json_string)
            good_json_string = repair_json(bad_json_string, return_objects=True)
            if isinstance(good_json_string, list):
                good_json_string = good_json_string[0]
            print(good_json_string)
            if db_id not in all_relations:
                all_relations[db_id] = [good_json_string["rel"]]
            else:
                all_relations[db_id].append(good_json_string["rel"])
            
            # Checkpoint 2: Save progress to file
            checkpoint = {
                "db_id": db_id,
                "pair_index": pair_idx + 1
            }
            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint, f)
    
    # Checkpoint 3: Remove checkpoint file after completion
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
    
    with open("data/join-relationship/join_rel.json", "w") as json_file:
        json.dump(all_relations, json_file)
        json_file.write("\n")

find_relationship()
# chain = LLMChain(llm=CHAT, prompt=CREATE_TRIPLE)
# all_relations = dict(
#     {
#         "farm": [
#             ["city", "farm", "hosted by"],
#             ["city", "farm_competition", "hosted by"],
#             ["city", "competition_record", "hosted by"],
#             ["farm", "farm_competition", "participates in"],
#             ["farm", "competition_record", "participates in"],
#             ["farm_competition", "competition_record", "related to"],
#             ["farm", "city", "located in"],
#             ["farm_competition", "city", "located in"],
#             ["competition_record", "city", "located in"],
#             ["farm_competition", "farm", "part of"],
#             ["competition_record", "farm", "part of"],
#             ["competition_record", "farm_competition", "part of"],
#         ],
#     }
# )
# with open("data/join-relationship/table_relations.json", "w") as json_file:
#     json.dump(all_relations, json_file)
#     json_file.write("\n")
# with open("data/join-relationship/table_tuples.json", "r") as f:
#     all_combinations = json.load(f)
# schema_info = convert_json_to_schema("data/tables.json")
# for db_id in all_combinations.keys():
#     if db_id == "farm":
#         continue


