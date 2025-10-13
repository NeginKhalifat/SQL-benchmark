import glob
import pandas as pd
import csv
import os
import sys


import importlib
import argparse
import json
from json_repair import repair_json


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test-suite-sql-eval-master')))
tsa = importlib.import_module("evaluation")
evaluator = tsa.Evaluator()
# Import evaluation module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test-suite-sql-eval-master')))
process_sql = importlib.import_module("process_sql")
Schema= process_sql.Schema
get_schema = process_sql.get_schema
get_sql = process_sql.get_sql
# Flags
DISABLE_VALUE = True
DISABLE_DISTINCT = True

# Argument parser
parser = argparse.ArgumentParser(description='Evaluate synthetic SQL benchmark')
parser.add_argument("--synthesis_method", help="Method to synthesize SQL queries", type=str, choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement','spider'], default='schema_guided')
parser.add_argument("--run_synthesize", help="Run synthesis_SQL", action='store_true')
parser.add_argument("--output_file", help="Output file for analysis results", type=str, required=True)
args = parser.parse_args()
print(args.output_file)
# Functions
def concat_all(folder_path, output_file):
    """Concatenate all CSV files in a folder into a single DataFrame."""
    combined_df = pd.concat(
        (pd.read_csv(file_path).assign(db_name=os.path.basename(file_path).replace('.csv', '').split("_res")[0])
         for file_path in glob.glob(os.path.join(folder_path, '*.csv'))),
        ignore_index=True
    )
    combined_df.to_csv(output_file, index=False)
    return combined_df

def calculate_statistics(data):
    """Calculate average statistics from the given data."""
    total_counts = {key: 0 for key in data[next(iter(data))]}
    total_sums = {key: 0 for key in total_counts}

    for subdict in data.values():
        for key, value in subdict.items():
            total_sums[key] += value
            total_counts[key] += 1

    return {key: (total_sums[key] / total_counts[key]) if total_counts[key] > 0 else 0 for key in total_sums}

# Main
if __name__ == "__main__":
    if args.synthesis_method=="spider":
        folder_path = f"data/dev22.json"
        statistics = {}
        # not_parsable = {}
        hardness_levels = {"easy": 0, "medium": 0, "hard": 0, "extra": 0, "table_1": 0, "table_2": 0, "table_3": 0, "table_4": 0,  "table_5": 0, "table_6": 0, "table_7": 0, "nested": 0, 'all': 0}
        # error_count =0
        correct_sqls = {}
        all = pd.read_json(folder_path)
        print(all)
        # Iterate over CSV files in the specified folder
        for i, instance in all.iterrows():
            # print('---------')

            # print(instance)
            
            db_name = instance['db_id']
            # if "_processed.csv" in db_name:
            #     db_name = db_name.split("_processed.csv")[0]

            # elif ".csv" in db_name:
            #     db_name = os.path.basename(file).split(".csv")[0]

            # # print(db_name)
            # if db_name not in val_dbs:
            #     continue
            if db_name not in correct_sqls:
                correct_sqls[db_name]=[]

                statistics[db_name] = hardness_levels.copy()
            

            db2 = os.path.join("test-suite-sql-eval-master/database/", db_name, f"{db_name}.sqlite")
            # print(db2)
            schema2 = Schema(get_schema(db2))
            query = instance["query"]
            # print(query)
            # print(type(query))
            
          
            g_sql = get_sql(schema2, query=query)
            count_table = len(g_sql["from"]["table_units"])
        
            hardness = evaluator.eval_hardness(g_sql)
            # print("HARDNESS", hardness)
            # print(tsa.count_component2(g_sql))
            correct_sqls[db_name].append({"sql":query, "hardness": hardness})
            statistics[db_name][hardness] += 1
            statistics[db_name][f"table_{count_table}"] += 1
            statistics[db_name]["all"] += 1
            if tsa.count_component2(g_sql) > 0:
                statistics[db_name]["nested"] += 1

            

        # Summary Statistics
        summary_stats = {key: sum(stats.get(key, 0) for stats in statistics.values()) for key in hardness_levels}
        averages = calculate_statistics(statistics)
        # Print and save summary results
        print("Total Statistics:", summary_stats)
        print("Averages for each category:")
        for key, avg in averages.items():
            print(f"{key}: {avg:.2f}")

        with open(args.output_file, 'w') as file:
            for key, avg in averages.items():
                file.write(f"{key}: {avg:.2f}\n")
            file.write(f"Total Statistics: {summary_stats}\n")

    else:
        folder_path = f"data/synthetic-queries/{args.synthesis_method}"
        val_dbs =[
        "concert_singer",
        "pets_1",
        "car_1",
        "flight_2",
        "employee_hire_evaluation",
        "cre_Doc_Template_Mgt",
        "course_teach",
        "museum_visit",
        "wta_1",
        "battle_death",
        "student_transcripts_tracking",
        "tvshow",
        "poker_player",
        "voter_1",
        "world_1",
        "orchestra",
        "network_1",
        "dog_kennels",
        "singer",
        "real_estate_properties"
    ]

        correct_sqls_folder = os.path.join(folder_path, "correct_sqls2")
        os.makedirs(correct_sqls_folder, exist_ok=True)

        print(folder_path)
        statistics = {}
        not_parsable = {}
        hardness_levels = {"easy": 0, "medium": 0, "hard": 0, "extra": 0, "table_1": 0, "table_2": 0, "table_3": 0, "table_4": 0,  "table_5": 0, "table_6": 0, "table_7": 0, "nested": 0, 'all': 0}
        error_count =0
        correct_sqls = {}
        # Iterate over CSV files in the specified folder
        for file in glob.glob(os.path.join(folder_path, '*.csv')):
            db_name = os.path.basename(file).split("_res.csv")[0]
            if "_processed.csv" in db_name:
                db_name = db_name.split("_processed.csv")[0]

            elif ".csv" in db_name:
                db_name = os.path.basename(file).split(".csv")[0]

            # print(db_name)
            if db_name not in val_dbs:
                continue
            correct_sqls[db_name]=[]

            statistics[db_name] = hardness_levels.copy()
            df = pd.read_csv(file)

            # Evaluate each query
            for instance in df.to_dict(orient="records"):
                db2 = os.path.join("test-suite-sql-eval-master/database/", db_name, f"{db_name}.sqlite")
                # print(db2)
                schema2 = Schema(get_schema(db2))
                query = instance["query"]
                # print(query)
                # print(type(query))
                if args.synthesis_method == "llm_based":
                    query = repair_json(query,return_objects=True)
                    # print(type(query))
                    # print(query["query"])
                    # query = json.loads(query)
                    query = query["query"]
                # print(query)
                
                try:
                    g_sql = get_sql(schema2, query=query)
                    count_table = len(g_sql["from"]["table_units"])
                
                    hardness = evaluator.eval_hardness(g_sql)
                    # print("HARDNESS", hardness)
                    # print(tsa.count_component2(g_sql))
                    correct_sqls[db_name].append({"sql":query, "hardness": hardness})
                    statistics[db_name][hardness] += 1
                    statistics[db_name][f"table_{count_table}"] += 1
                    statistics[db_name]["all"] += 1
                    if tsa.count_component2(g_sql) > 0:
                        statistics[db_name]["nested"] += 1

                except Exception as e:
                    if "." not in str(e):
                        print(e)
                        error_count+=1
                        print("NOOOOO: ", error_count)
                        print(query)
                        print(db2)
                    not_parsable.setdefault(db_name, []).append(query)
        for db_name, sql_list in correct_sqls.items():
            output_file_path = os.path.join(correct_sqls_folder, f"{db_name}_correct_sqls.csv")
            with open(output_file_path, 'w', newline='', encoding='utf-8') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=["sql", "hardness"])
                writer.writeheader()
                writer.writerows(sql_list)

        # Summary Statistics
        summary_stats = {key: sum(stats.get(key, 0) for stats in statistics.values()) for key in hardness_levels}
        averages = calculate_statistics(statistics)
        print("len(not_parsable)", len(not_parsable))
        # Print and save summary results
        print("Total Statistics:", summary_stats)
        print("Averages for each category:")
        for key, avg in averages.items():
            print(f"{key}: {avg:.2f}")

        with open(args.output_file, 'w') as file:
            for key, avg in averages.items():
                file.write(f"{key}: {avg:.2f}\n")
            file.write(f"Total Statistics: {summary_stats}\n")
            file.write(f"Error in Parsing: {not_parsable}\n")
