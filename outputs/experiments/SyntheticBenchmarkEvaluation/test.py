import json
with open("/home/khalifat/code/sql-generator/outputs/experiments/SQL2text/llm_based/combined_filtered.json","r") as f:
    data = json.load(f)
print(len(data))