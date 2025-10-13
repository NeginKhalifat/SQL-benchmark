
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
import matplotlib.pyplot as plt
import numpy as np

# Data from the files (Total Statistics)
categories_hardness = ["Easy", "Medium", "Hard", "Extra", "Nested"]

llm_based_hardness = [34, 98, 100, 213, 131]
schema_guided_hardness = [38, 148, 104, 210, 126]
schema_guided_overview_hardness = [29, 155, 108, 258, 157]

# Compute percentages
total_hardness = np.array(llm_based_hardness) + np.array(schema_guided_hardness) + np.array(schema_guided_overview_hardness)

llm_based_hardness_pct = (np.array(llm_based_hardness) / total_hardness) * 100
schema_guided_hardness_pct = (np.array(schema_guided_hardness) / total_hardness) * 100
schema_guided_overview_hardness_pct = (np.array(schema_guided_overview_hardness) / total_hardness) * 100

# Define colors suitable for colorblind-friendly visualization
colors = ["#4477AA", "#DDCC77", "#117733"]  # Blue, Yellow, Green

# Create a figure for Hardness Level distribution
plt.figure(figsize=(12, 7))  # Larger figure size for better readability
bar_width = 0.2  # Balanced bar width
x = np.arange(len(categories_hardness))

# Plot bars
bars1 = plt.bar(x - bar_width, llm_based_hardness_pct, bar_width, label="LLM-Based", color=colors[0])
bars2 = plt.bar(x, schema_guided_hardness_pct, bar_width, label="Schema+LLM", color=colors[1])
bars3 = plt.bar(x + bar_width, schema_guided_overview_hardness_pct, bar_width, label="Schema-Only", color=colors[2])

# Add data labels on top of the bars
# for bars in [bars1, bars2, bars3]:
#     for bar in bars:
#         height = bar.get_height()
#         plt.text(bar.get_x() + bar.get_width() / 2, height + 1, f"{height:.1f}%", ha='center', fontsize=12)

# Labels and Formatting
plt.xlabel("Hardness Level", fontsize=16, fontweight='bold')
plt.ylabel("Percentage (%)", fontsize=16, fontweight='bold')
plt.title("Query Distribution by Hardness Level", fontsize=18, fontweight='bold')
plt.xticks(x, categories_hardness, fontsize=14)
plt.yticks(fontsize=14)

# Adjust legend placement to below the chart
plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3, fontsize=14, frameon=False)

plt.tight_layout()

# Save as high-resolution image
plt.savefig("query_hardness_distribution_readable.png", dpi=400)  # High DPI for clarity in print
plt.show()
