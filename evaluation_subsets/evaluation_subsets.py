import os
import pandas as pd
import argparse
import json

# Argument parser for synthesis method
parser = argparse.ArgumentParser(description='Evaluate Conversion of SQL to NLQ')
parser.add_argument(
    "--synthesis_method",
    help="Method to synthesize SQL queries",
    type=str,
    choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement'],
    default='schema_guided'
)
args = parser.parse_args()

# Step 1: Combine all files into a single DataFrame
directory = f"outputs/experiments/SQL2text/{args.synthesis_method}"  # Replace with the path to your directory
all_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
output_folder = f"outputs/experiments/SQL2text/{args.synthesis_method}/evaluation_subsets/"
os.makedirs(output_folder, exist_ok=True)

combined_data = []
dbs = [
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
for file in all_files:
    db_name = file.replace(".csv", "")  # Extract db_name from the file name
    if db_name not in dbs:
        continue
    file_path = os.path.join(directory, file)
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Filter out rows where 'flag' is False
    df = df[df['flag'] != False]
    
    # Add db_name column
    df["db_id"] = db_name
    
    # Append filtered rows as dictionaries to the combined_data list
    combined_data.extend(df.to_dict(orient="records"))

# Step 2: Save the combined data to a JSON file
output_file = os.path.join(output_folder, f"combined_filtered_{args.synthesis_method}.json")
print(len(combined_data))
# Save as valid JSON
with open(output_file, "w") as json_file:
    json.dump(combined_data, json_file, indent=4)

print(f"Combined and filtered JSON file saved at: {output_file}")
