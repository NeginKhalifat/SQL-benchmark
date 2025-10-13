import os
import sys
from pathlib import Path

import argparse
import importlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches  # For custom legend entries
import pandas as pd

# Add test-suite-sql-eval module to path
REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_SUITE_PATH = REPO_ROOT / "test-suite-sql-eval-master"
sys.path.insert(0, str(TEST_SUITE_PATH))
tsa = importlib.import_module("evaluation")
evaluator = tsa.Evaluator()
process_sql = importlib.import_module("process_sql")
Schema = process_sql.Schema
get_schema = process_sql.get_schema
get_sql = process_sql.get_sql

# Argument parser for synthesis method
parser = argparse.ArgumentParser(description='Evaluate Conversion of SQL to NLQ')
parser.add_argument("--synthesis_method", help="Method to synthesize SQL queries", type=str, choices=['schema_guided', 'llm_based', 'schema_guided_llm_refinement'], default='schema_guided')
args = parser.parse_args()

# Directory containing CSV files
directory = REPO_ROOT / "outputs" / "experiments" / "SQL2text" / args.synthesis_method
directory.mkdir(parents=True, exist_ok=True)

# File to write output
output_file = REPO_ROOT / "outputs" / "experiments" / "SQL2text" / f"summary_{args.synthesis_method}.txt"
print(f"Output will be saved to: {output_file}")

# Initialize statistics
results = {}
total_success = 0
total_failures = 0
total_queries = 0
overall_iteration_success = [0] * 5  # Max 5 iterations

# Hardness tracking
hardness_levels = ["easy", "medium", "hard", "extra"]
hardness_success = {level: [0] * 5 for level in hardness_levels}
hardness_total = {level: 0 for level in hardness_levels}
dbs = [
    "concert_singer", "pets_1", "car_1", "flight_2", "employee_hire_evaluation",
    "cre_Doc_Template_Mgt", "course_teach", "museum_visit", "wta_1", "battle_death",
    "student_transcripts_tracking", "tvshow", "poker_player", "voter_1", "world_1",
    "orchestra", "network_1", "dog_kennels", "singer", "real_estate_properties"
]

# Process each file in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        db_name = filename.split(".")[0]
        if db_name not in dbs:
            continue
        file_path = os.path.join(directory, filename)
        
        # Read the CSV file without overwriting the header
        df = pd.read_csv(file_path)
        
        db_path = os.path.join("test-suite-sql-eval-master/database/", db_name, f"{db_name}.sqlite")
        schema = Schema(get_schema(db_path))

        # Add a column for hardness
        hardness_column = []
        for _, row in df.iterrows():
            if row["iter"] == "iter":
                continue  # Skip invalid rows (if there are non-query rows like headers in the data)
            
            try:
                g_sql = get_sql(schema, query=row["query"])
                hardness = evaluator.eval_hardness(g_sql)  # Calculate hardness level
                hardness_column.append(hardness)  # Append the hardness level to the list
            except Exception as e:
                print(f"Error processing query: {row['query']}\n{e}")
                hardness_column.append("unknown")  # Append "unknown" for errors

        # Add the hardness column to the DataFrame
        df["hardness"] = hardness_column

        # Save the updated DataFrame back to the same file
        updated_file_path = os.path.join(directory, f"{db_name}.csv")
        df.to_csv(updated_file_path, index=False)
        print(f"Updated file saved: {updated_file_path}")

# Process each file in the directory for statistics
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        db_name = filename.split(".")[0]
        if db_name not in dbs:
            continue
        file_path = os.path.join(directory, filename)
        df = pd.read_csv(file_path, header=None, names=["query", "question", "iter", "flag", "hardness"])

        iteration_success = [0] * 5
        file_total = len(df)
        db_success = 0
        db_failures = 0

        db_path = os.path.join("test-suite-sql-eval-master/database/", db_name, f"{db_name}.sqlite")
        schema = Schema(get_schema(db_path))

        for _, row in df.iterrows():
            if row["iter"] == "iter":
                continue

            hardness = row["hardness"]
            iter_num = int(row["iter"]) - 2
            if iter_num < 0:
                continue

            flag = row["flag"]
            
            if flag == "True" and iter_num < 5:
                hardness_success[hardness][iter_num] += 1
                iteration_success[iter_num] += 1
                overall_iteration_success[iter_num] += 1
                db_success += 1

            elif flag == "False":
                db_failures += 1

            hardness_total[hardness] += 1

        total_queries += file_total
        total_success += db_success
        total_failures += db_failures

        results[db_name] = {
            "total_queries": file_total,
            "success": db_success,
            "failures": db_failures,
            "iteration_success": iteration_success
        }

# Write results to the output file
with open(output_file, "w") as f:
    for db_name, stats in results.items():
        f.write(f"Database: {db_name}\n")
        f.write(f"  Total Queries: {stats['total_queries']}\n")
        f.write(f"  Total Successful Conversions: {stats['success']} ({(stats['success'] / stats['total_queries'] * 100):.2f}%)\n")
        f.write(f"  Total Failed Conversions: {stats['failures']} ({(stats['failures'] / stats['total_queries'] * 100):.2f}%)\n")
        f.write("  Success by Iteration:\n")
        for i, count in enumerate(stats["iteration_success"]):
            f.write(f"    Iteration {i + 1}: {count}\n")
        f.write("-" * 40 + "\n")

    f.write("Overall Summary:\n")
    f.write(f"Total Queries: {total_queries}\n")
    f.write(f"Total Successful Conversions: {total_success} ({(total_success / total_queries * 100):.2f}%)\n")
    f.write(f"Total Failed Conversions: {total_failures} ({(total_failures / total_queries * 100):.2f}%)\n")

    # Add hardness level analysis to the summary
    f.write("\nHardness Level Analysis:\n")
    for level in hardness_levels:
        success_count = sum(hardness_success[level])
        total_count = hardness_total[level]
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
        f.write(f"  {level.capitalize()}:\n")
        f.write(f"    Total Queries: {total_count}\n")
        f.write(f"    Successful Conversions: {success_count} ({success_rate:.2f}%)\n")

# Generate cumulative success by iteration
overall_accumulated_success = [sum(overall_iteration_success[:i + 1]) for i in range(len(overall_iteration_success))]
hardness_accumulated_success = {
    level: [sum(success[:i + 1]) for i in range(len(success))]
    for level, success in hardness_success.items()
}

# Calculate total queries per hardness level
hardness_total_queries = {level: sum(hardness_success[level]) for level in hardness_levels}

# Normalize cumulative success for each hardness level
normalized_hardness_success = {
    level: [count / hardness_total_queries[level] if hardness_total_queries[level] > 0 else 0 for count in hardness_accumulated_success[level]]
    for level in hardness_levels
}

# Generate plot for normalized success rates
plt.figure(figsize=(14, 8))

# Plot overall success rate
plt.plot(
    range(1, 6),
    [s / total_queries * 100 for s in overall_accumulated_success],  # Normalize overall success to percentage
    marker='o',
    label='Overall Success Rate (%)',
    linestyle='-',
    linewidth=2,
    markersize=8
)

# Plot success rates for each hardness level
for level, normalized_success in normalized_hardness_success.items():
    plt.plot(
        range(1, 6),
        [rate * 100 for rate in normalized_success],  # Convert to percentage
        marker='o',
        label=f'{level.capitalize()} Success Rate (%)',
        linestyle='--',
        linewidth=2,
        markersize=8
    )

# Annotate the highest success rate for each hardness level
for level, normalized_success in normalized_hardness_success.items():
    max_rate = max(normalized_success) * 100
    max_iter = normalized_success.index(max(normalized_success)) + 1
    plt.annotate(
        f'{max_rate:.1f}%',
        (max_iter, max_rate),
        textcoords="offset points",
        xytext=(0, 10),
        ha='center',
        fontsize=10,
        color='black'
    )

# Customize the plot
plt.xlabel('Iteration Number', fontsize=14, weight='bold')
plt.ylabel('Cumulative Success Rate (%)', fontsize=14, weight='bold')
if args.synthesis_method=="schema_guidede_llm_refinement":
    plt.title(f'Cumulative Success Rate by Iteration and Query Complexity (CLAUSE_BASED_WITH_LLM_REFINEMENT)', fontsize=16, weight='bold')

plt.title(f'Cumulative Success Rate by Iteration and Query Complexity ({args.synthesis_method.capitalize()})', fontsize=16, weight='bold')

# Customize ticks
plt.xticks(range(1, 6), fontsize=12)
plt.yticks(fontsize=12)

# Add gridlines for clarity
plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)

# Add legend
plt.legend(fontsize=12, loc='lower right')

# Tighten layout
plt.tight_layout()

# Save the figure with a caption in the paper
figure_path = f"outputs/experiments/SQL2text/explainable_cumulative_success_{args.synthesis_method}.png"
plt.savefig(figure_path, dpi=300, bbox_inches="tight")
print(f"Explainable figure saved at {figure_path}")

# Hardness level data
success_rates = [
    (sum(hardness_success[level]) / hardness_total[level] * 100) if hardness_total[level] > 0 else 0
    for level in hardness_levels
]
total_queries = [hardness_total[level] for level in hardness_levels]

# Create figure and axis for the bar plot
fig, ax1 = plt.subplots(figsize=(14, 8))

# Bar plot for success rates
bars = ax1.bar(
    hardness_levels,
    success_rates,
    color=['#4caf50', '#ffeb3b', '#ff9800', '#f44336'],  # Multicolored bars for hardness levels
    alpha=0.8,
    label="Success Rate (%) (Bars)"
)

# Annotate success rates on the bars
for bar, rate in zip(bars, success_rates):
    ax1.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() - 15,
        f"{rate:.2f}%",
        ha='center',
        va='bottom',
        fontsize=12,
        color='white',
        weight='bold'
    )

# Set labels, title, and styling for the first y-axis
ax1.set_xlabel("Query Hardness Level", fontsize=14, weight='bold')
ax1.set_ylabel("Success Rate (%)", fontsize=14, weight='bold', color='black')
ax1.tick_params(axis='y', labelcolor='black')
ax1.set_ylim(0, 100)

# Create a secondary y-axis for total queries
ax2 = ax1.twinx()
line, = ax2.plot(
    hardness_levels,
    total_queries,
    color='blue',
    marker='o',
    linestyle='--',
    linewidth=2,
    label="Total Queries (Line)"
)

# Annotate total queries on the line plot
print(total_queries)
for i, total in enumerate(total_queries):
    ax2.text(
        i,
        total +1,
        str(total),
        ha='center',
        fontsize=12,
        color='blue'
    )

# Set labels, title, and styling for the second y-axis
ax2.set_ylabel("Total Queries", fontsize=14, weight='bold', color='blue')
ax2.tick_params(axis='y', labelcolor='blue')

# Add gridlines for better readability
ax1.grid(axis='y', linestyle='--', alpha=0.7)

# Add a title
plt.title(
    "Hardness Level Analysis: Success Rates and Total Queries",
    fontsize=16,
    weight='bold'
)

# Add a combined legend with both bar and line representations
plt.legend(
    [bars, line],
    ["Success Rate (%) (Bars)", "Total Queries (Line)"],
    loc='upper center',
    bbox_to_anchor=(0.5, 0.92),
    ncol=1,
    fontsize=12
)

# Adjust layout to provide spacing
plt.tight_layout(pad=2)  # Add extra padding for title and legend

# Save the figure
hardness_figure_path = f"outputs/experiments/SQL2text/hardness_level_analysis_{args.synthesis_method}.png"
plt.savefig(hardness_figure_path, dpi=300, bbox_inches="tight")
print(f"Hardness Level Analysis Figure saved at {hardness_figure_path}")