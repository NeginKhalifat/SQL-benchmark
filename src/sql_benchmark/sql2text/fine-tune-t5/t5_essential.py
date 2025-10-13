import json

import pandas as pd
from tqdm import tqdm  # Import tqdm

# from sql2text.rule_based.convertor import convert_sql_to_text as sql2textconvertor



def load_dataset(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def load_table_relations(file_path):
    table_relations = {}
    with open(file_path, "r") as f:
        for line in f:
            table_relation = json.loads(line)
            if isinstance(list(table_relation.values())[0][0], dict):
                table_relation = {
                    key: [[d["entity1"], d["entity2"], d["verb"]] for d in value]
                    for key, value in table_relation.items()
                }
            table_relations.update(table_relation)
    return table_relations


def process_data(data, table_relations, all_schema):
    processed_data = []
    for i in tqdm(range(len(data))):  # Use tqdm to visualize the waiting time
        db_name = data[i]["db_id"]
        if db_name not in table_relations:
            continue
        schema_info = all_schema.query(f"db_id == '{db_name}'")
        text = sql2textconvertor(
            schema_info=schema_info,
            table_file="data/tables.json",
            db_id=db_name,
            sql=data[i]["query"],
            pasrsed_sql=None,
            join_rel=table_relations[db_name],
        )
        removed_order_by_limit = text.split(" The result ")[0]
        processed_data.append(
            {
                "query": data[i]["query"],
                "rule_based_text_wo_orderby_limit": removed_order_by_limit,
                "rule_based_text": text,
                "gt_question": data[i]["question"],
            }
        )
    dataset = pd.DataFrame(processed_data)
    return dataset


# Load dataset
train_ds = load_dataset("data/train_spider.json")
eval_ds = load_dataset("data/dev.json")
print(len(train_ds), len(eval_ds   ))
# all_schema = pd.read_json("data/tables.json")
# table_relations = load_table_relations("data/join-relationship/table_relations.json")

# train_dataset = process_data(train_ds, table_relations, all_schema)
# eval_dataset = process_data(eval_ds, table_relations, all_schema)

# train_dataset.to_csv(
#     "sql2text/rule_based/fine_tuning_t5/dataset/train_dataset.csv", index=False
# )
# eval_dataset.to_csv(
#     "sql2text/rule_based/fine_tuning_t5/dataset/eval_dataset.csv", index=False
# )
