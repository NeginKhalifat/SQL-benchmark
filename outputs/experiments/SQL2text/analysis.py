import matplotlib.pyplot as plt

# --- 1) Hardcode Your Per-Database Results ---
# Approach 1: "LLM-based" data
approach1_data = [
    {
        "db_name": "cre_Doc_Template_Mgt",
        "total_queries": 31,
        "iteration_success": [7, 6, 4, 3, 1]
    },
    {
        "db_name": "employee_hire_evaluation",
        "total_queries": 20,
        "iteration_success": [5, 4, 2, 0, 2]
    },
    {
        "db_name": "real_estate_properties",
        "total_queries": 22,
        "iteration_success": [8, 0, 0, 1, 0]
    },
    {
        "db_name": "dog_kennels",
        "total_queries": 22,
        "iteration_success": [5, 1, 1, 1, 1]
    },
    {
        "db_name": "tvshow",
        "total_queries": 23,
        "iteration_success": [3, 6, 0, 2, 2]
    },
    {
        "db_name": "network_1",
        "total_queries": 22,
        "iteration_success": [3, 5, 0, 1, 0]
    },
    {
        "db_name": "course_teach",
        "total_queries": 21,
        "iteration_success": [6, 6, 1, 1, 0]
    },
    {
        "db_name": "orchestra",
        "total_queries": 17,
        "iteration_success": [8, 2, 0, 1, 0]
    },
    {
        "db_name": "student_transcripts_tracking",
        "total_queries": 27,
        "iteration_success": [5, 4, 2, 3, 1]
    },
    {
        "db_name": "wta_1",
        "total_queries": 23,
        "iteration_success": [4, 1, 2, 0, 0]
    },
    {
        "db_name": "museum_visit",
        "total_queries": 21,
        "iteration_success": [6, 1, 1, 2, 0]
    },
    {
        "db_name": "concert_singer",
        "total_queries": 22,
        "iteration_success": [10, 1, 2, 0, 1]
    },
    {
        "db_name": "poker_player",
        "total_queries": 23,
        "iteration_success": [5, 5, 1, 0, 1]
    },
    {
        "db_name": "car_1",
        "total_queries": 27,
        "iteration_success": [6, 0, 1, 2, 1]
    },
    {
        "db_name": "battle_death",
        "total_queries": 22,
        "iteration_success": [7, 0, 1, 0, 1]
    },
    {
        "db_name": "singer",
        "total_queries": 26,
        "iteration_success": [11, 1, 1, 2, 3]
    },
    {
        "db_name": "pets_1",
        "total_queries": 22,
        "iteration_success": [6, 5, 0, 1, 1]
    },
    {
        "db_name": "flight_2",
        "total_queries": 25,
        "iteration_success": [3, 7, 2, 1, 1]
    },
    {
        "db_name": "voter_1",
        "total_queries": 23,
        "iteration_success": [2, 1, 0, 1, 1]
    },
    {
        "db_name": "world_1",
        "total_queries": 23,
        "iteration_success": [9, 4, 2, 1, 1]
    }
]

# Approach 2: "Clause-based-LLM-Ref." data
approach2_data = [
    {
        "db_name": "cre_Doc_Template_Mgt",
        "total_queries": 29,
        "iteration_success": [8, 3, 3, 0, 1]
    },
    {
        "db_name": "employee_hire_evaluation",
        "total_queries": 25,
        "iteration_success": [4, 6, 5, 0, 1]
    },
    {
        "db_name": "real_estate_properties",
        "total_queries": 21,
        "iteration_success": [3, 4, 0, 0, 0]
    },
    {
        "db_name": "dog_kennels",
        "total_queries": 30,
        "iteration_success": [5, 2, 2, 3, 2]
    },
    {
        "db_name": "tvshow",
        "total_queries": 19,
        "iteration_success": [4, 1, 1, 2, 0]
    },
    {
        "db_name": "network_1",
        "total_queries": 29,
        "iteration_success": [4, 2, 3, 2, 1]
    },
    {
        "db_name": "course_teach",
        "total_queries": 18,
        "iteration_success": [6, 3, 1, 1, 1]
    },
    {
        "db_name": "orchestra",
        "total_queries": 25,
        "iteration_success": [7, 2, 0, 1, 1]
    },
    {
        "db_name": "student_transcripts_tracking",
        "total_queries": 32,
        "iteration_success": [10, 5, 2, 2, 0]
    },
    {
        "db_name": "wta_1",
        "total_queries": 21,
        "iteration_success": [2, 3, 2, 1, 1]
    },
    {
        "db_name": "museum_visit",
        "total_queries": 27,
        "iteration_success": [10, 1, 0, 0, 2]
    },
    {
        "db_name": "concert_singer",
        "total_queries": 25,
        "iteration_success": [7, 2, 3, 1, 1]
    },
    {
        "db_name": "poker_player",
        "total_queries": 26,
        "iteration_success": [1, 1, 2, 3, 2]
    },
    {
        "db_name": "car_1",
        "total_queries": 29,
        "iteration_success": [5, 3, 3, 3, 2]
    },
    {
        "db_name": "battle_death",
        "total_queries": 26,
        "iteration_success": [4, 2, 1, 3, 2]
    },
    {
        "db_name": "singer",
        "total_queries": 30,
        "iteration_success": [9, 1, 2, 2, 2]
    },
    {
        "db_name": "pets_1",
        "total_queries": 23,
        "iteration_success": [11, 1, 1, 0, 1]
    },
    {
        "db_name": "flight_2",
        "total_queries": 23,
        "iteration_success": [6, 3, 1, 0, 2]
    },
    {
        "db_name": "voter_1",
        "total_queries": 31,
        "iteration_success": [9, 3, 2, 0, 0]
    },
    {
        "db_name": "world_1",
        "total_queries": 26,
        "iteration_success": [4, 0, 2, 1, 1]
    }
]

def compute_cumulative_rates(db_list):
    """Return cumulative success rates (%) after 5 iterations, 
       plus iteration-wise new success sums, plus total query count.
    """
    iteration_sum = [0] * 5
    total_queries = 0
    for db in db_list:
        total_queries += db["total_queries"]
        for i, val in enumerate(db["iteration_success"]):
            iteration_sum[i] += val

    cumulative_success = []
    running_sum = 0
    for val in iteration_sum:
        running_sum += val
        cumulative_success.append(running_sum)

    # Convert to success rate (percentage)
    cumulative_rates = [x / total_queries * 100 for x in cumulative_success]
    return cumulative_rates, iteration_sum, total_queries

# Compute data for each approach
approach1_rates, approach1_iter_sum, approach1_total = compute_cumulative_rates(approach1_data)
approach2_rates, approach2_iter_sum, approach2_total = compute_cumulative_rates(approach2_data)

iterations = [1, 2, 3, 4, 5]

# Create the figure
plt.figure(figsize=(7, 4))

# Plot Approach 1
plt.plot(
    iterations,
    approach1_rates,
    marker='o',
    markersize=7,
    linewidth=2,
    linestyle='-',
    label='LLM-based'
)
# Annotate Approach 1 with small offsets
for i, rate in enumerate(approach1_rates):
    plt.annotate(
        f"{rate:.1f}%",
        xy=(iterations[i], rate),
        xytext=(0, 8),  # shift upwards by 8 points
        textcoords='offset points',
        ha='center',
        fontsize=9
    )

# Plot Approach 2
plt.plot(
    iterations,
    approach2_rates,
    marker='s',
    markersize=7,
    linewidth=2,
    linestyle='--',
    label='Clause-based-LLM-Ref.'
)
# Annotate Approach 2 with small offsets
for i, rate in enumerate(approach2_rates):
    plt.annotate(
        f"{rate:.1f}%",
        xy=(iterations[i], rate),
        xytext=(0, -15),  # shift down by 15 points
        textcoords='offset points',
        ha='center',
        fontsize=9
    )

# Axis labels & Title
plt.xlabel('Iteration #', fontsize=12)
plt.ylabel('Cumulative Success Rate (%)', fontsize=12)
plt.title('Comparison of Cumulative Success Rates', fontsize=14)

# Ticks, Grid, Legend
plt.xticks(iterations)
plt.ylim([0, 105])
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(loc='lower right', fontsize=11)

plt.tight_layout()
plt.savefig("comparison_of_cumulative_success_rates.png", dpi=300, bbox_inches="tight")
plt.show()

# Print a short summary
final_success_1 = approach1_rates[-1]
final_success_2 = approach2_rates[-1]

print("=== Approach 1: LLM-based ===")
print(f"  Total Queries: {approach1_total}")
print(f"  Final Success Rate: {final_success_1:.2f}%")
print(f"  Iteration-wise new successes: {approach1_iter_sum}\n")

print("=== Approach 2: Clause-based-LLM-Ref. ===")
print(f"  Total Queries: {approach2_total}")
print(f"  Final Success Rate: {final_success_2:.2f}%")
print(f"  Iteration-wise new successes: {approach2_iter_sum}\n")
