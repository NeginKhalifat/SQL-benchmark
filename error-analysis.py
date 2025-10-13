import re

# Define the input log file name
log_file = "llm_based_gpt4_turbo_res.txt"
log_file = "llm_based_dail_res.txt"

log_file = "llm_based_din_res.txt"
level = "easy"
# Regular expressions to capture Execution Error Medium sections
execution_error_pattern = re.compile(f"@@@@@@@@@@@@@@@@@@@@@@\nExecution error {level}\nPredicted SQL: (.*?)\nGold SQL: (.*?)\nQuestion: (.*?)\n@@@@@@@@@@@@@@@@@@@@@@", re.DOTALL)

# Dictionary to store extracted queries and questions
execution_errors = []

# Read the log file and extract relevant information
with open(log_file, "r", encoding="utf-8") as file:
    log_content = file.read()
    matches = execution_error_pattern.findall(log_content)
    
    for match in matches:
        predicted_sql, gold_sql, question = match

        execution_errors.append({
            "Predicted SQL": predicted_sql.strip(),
            "Gold SQL": gold_sql.strip(),
            "Question": question.strip()
        })

# Print extracted queries and questions
for idx, error in enumerate(execution_errors, start=1):
    print(f"Execution Error {idx}:")
    print(f"Predicted SQL: {error['Predicted SQL']}")
    print(f"Gold SQL: {error['Gold SQL']}")
    print(f"Question: {error['Question']}")
    print("-" * 80)
i =1
questions = []
# Optionally, save the extracted data to a file
output_file = "outputs/experiments/errorAnalysis/"+f"{level}_"+log_file
print(output_file)
with open(output_file, "w", encoding="utf-8") as file:
    for error in execution_errors:
        file.write(f"{i}:\n")
        i+=1
        file.write(f"Predicted SQL: {error['Predicted SQL']}\n")
        file.write(f"Gold SQL: {error['Gold SQL']}\n")
        file.write(f"Question: {error['Question']}\n")
        questions.append(error['Question'])
        file.write("-" * 80 + "\n")
    file.write(str(questions))

print(f"Extracted Execution Error Medium queries saved to {output_file}.")
