import os
import pandas as pd
import random
from collections import defaultdict
import argparse
import json
parser = argparse.ArgumentParser(description='Evaluate Conversion of SQL to NLQ')
parser.add_argument("--synthesis_method", help="Method to synthesize SQL queries", type=str, choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement'], default='schema_guided')
args = parser.parse_args()
# Step 1: Combine all files into a single DataFrame
# directory = f"outputs/experiments/SQL2text/{args.synthesis_method}" # Replace with the path to your directory
# all_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
output_folder = f"outputs/experiments/SQL2text/{args.synthesis_method}/evaluation_subsets/"
os.makedirs(output_folder, exist_ok=True)
dataset1 = "combined_filtered_llm_based.json"
dataset2 = "combined_filtered_schema_guided_llm_refinement.json"
with open(dataset1, 'r') as f:
    data1 = json.load(f)
with open(dataset2, 'r') as f:
    data2 = json.load(f)
print(len(data1))
combined_data = []

for file in [1]:
    db_name = file.replace(".csv", "")  # Extract db_name from the file name
    file_path = os.path.join(directory, file)
    df = pd.read_csv(file_path)
    df["db_name"] = db_name  # Add db_name column
    combined_data.append(df)

# Combine all DataFrames

full_dataset = pd.concat(combined_data, ignore_index=True)
print(f"Combined {len(all_files)} files into a dataset with {len(full_dataset)} rows.")

# Step 2: Create the evaluation subset
subset_size = 438  # Total number of queries in the evaluation subset
hardness_levels = ["easy", "medium", "hard", "extra"]

# Proportional sampling based on hardness
hardness_proportions = {
    "easy": len(full_dataset[full_dataset["hardness"] == "easy"]) / len(full_dataset),
    "medium": len(full_dataset[full_dataset["hardness"] == "medium"]) / len(full_dataset),
    "hard": len(full_dataset[full_dataset["hardness"] == "hard"]) / len(full_dataset),
    "extra": len(full_dataset[full_dataset["hardness"] == "extra"]) / len(full_dataset),
}

# Calculate the number of queries to sample per hardness level
hardness_samples = {key: int(subset_size * proportion) for key, proportion in hardness_proportions.items()}

# Sample queries for the evaluation subset
print("Sampling queries for the evaluation subset...")
evaluation_subset = []
print(hardness_samples)
for hardness in hardness_levels:
    num_samples = hardness_samples[hardness]
    hardness_df = full_dataset[full_dataset["hardness"] == hardness]
    sampled_queries = hardness_df.sample(num_samples, random_state=42)
    evaluation_subset.append(sampled_queries)

evaluation_subset_df = pd.concat(evaluation_subset)
print(f"Created an evaluation subset with {len(evaluation_subset_df)} queries.")

# Step 3: Preprocess for Faster Lookups
print("Preprocessing for faster assignments...")
evaluation_subset_df["query_id"] = evaluation_subset_df.index  # Add unique query IDs
queries_by_db = evaluation_subset_df.groupby("db_name")["query_id"].apply(list).to_dict()
query_hardness = evaluation_subset_df.set_index("query_id")["hardness"].to_dict()
print(len(queries_by_db))
# Step 4: Assign queries to students
students = 380  # Total number of students
queries_per_student = 10  # Number of queries each student evaluates
evaluations_per_query = 10  # Each query must be evaluated by at least 4 students

assignments = defaultdict(list)  # Dictionary to store student assignments
query_evaluations = defaultdict(int)  # Track how many times each query is assigned
student_db_preferences = defaultdict(set)  # Track db_name preferences for each student
print("Assigning queries to students...")
print("Assigning queries to students...")
j = 0
for student in range(1, students + 1):
    print(f"Assigning queries to student {student}...")
    assigned_queries = []

    while len(assigned_queries) < queries_per_student:
        # Check for available databases
        available_dbs = [db for db in queries_by_db if len(queries_by_db[db]) > 0]
        print(available_dbs)
        if not available_dbs:
            assigned_queries = assignments[j]
            assignments[student] = assigned_queries
            j+=1
            continue
            
            

        # Select a database (prioritize student's preference if possible)
        preferred_db_names = list(student_db_preferences[student])
        if preferred_db_names:
            available_preferred_dbs = [db for db in preferred_db_names if db in available_dbs]
            db_name = random.choice(available_preferred_dbs) if available_preferred_dbs else random.choice(available_dbs)
        else:
            db_name = random.choice(available_dbs)
        print(f"[[{db_name}]]")

        # Fetch available queries from the selected db_name
        available_queries = [
            q for q in queries_by_db[db_name]
            if query_evaluations[q] < evaluations_per_query
        ]
        
        if not available_queries:
            print(f"No available queries in {db_name} for student {student}.")
            continue

        # Assign queries from the selected db_name
        while available_queries and len(assigned_queries) < queries_per_student:
            query = available_queries.pop(0)  # Pop the easiest query
            assigned_queries.append((query, db_name))
            query_evaluations[query] += 1
            queries_by_db[db_name].remove(query)

        # Update student's db_name preferences
        student_db_preferences[student].add(db_name)

    # Store the student's assignments
    assignments[student] = assigned_queries
    print(f"Student {student}: Assigned {len(assigned_queries)} queries.")
    if student % 10 == 0:
        print(f"Assigned queries to {student} students.")

    # Debugging: Print remaining queries
    total_remaining_queries = sum(len(queries) for queries in queries_by_db.values())
    print(f"Total remaining queries: {total_remaining_queries}")

print("Finished assigning queries to all students.")


# Step 5: Save the assignments
print("Saving assignments to CSV...")
final_assignments = []

for student, queries in assignments.items():
    for query, db_name in queries:
        hardness = query_hardness[query]
        final_assignments.append({"student_id": student, "query_id": query, "db_name": db_name, "hardness": hardness})

assignments_df = pd.DataFrame(final_assignments)
assignments_df.to_csv(f"{output_folder}/student_query_assignments_optimized.csv", index=False)
print("Student assignments saved to student_query_assignments_optimized.csv")
print("Saving assignments to CSV...")
final_assignments = []

for student, queries in assignments.items():
    for query, db_name in queries:
        row = evaluation_subset_df.loc[query]  # Fetch the row details from the evaluation subset
        final_assignments.append({
            "student_id": student,
            "query_id": query,
            "db_name": db_name,
            "query": row["query"],
            "question": row["question"],
            "hardness": row["hardness"]
        })

# Convert to DataFrame and save to CSV
assignments_df = pd.DataFrame(final_assignments)
assignments_df.to_csv(f"{output_folder}/student_query_assignments_optimized.csv", index=False)
print("Student assignments saved to student_query_assignments_optimized.csv")