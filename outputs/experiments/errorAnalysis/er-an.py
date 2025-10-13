import json
level ="easy"
predicted={}
def extract_questions_gold_queries(file_path):
    """
    Extracts questions and their corresponding gold queries from the given file.
    """
    questions_gold_queries = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read().split("--------------------------------------------------------------------------------")  # Separate each block
        predicted[file_path]={}
        for block in data:
            lines = block.strip().split("\n")
            question = None
            gold_sql = None

            for line in lines:
                if line.startswith("Question:"):
                    question = line.replace("Question:", "").strip()
                elif line.startswith("Gold SQL:"):
                    gold_sql = line.replace("Gold SQL:", "").strip()
                
                elif line.startswith("Predicted SQL:"):
                    p = line.replace("Predicted SQL:", "").strip()
            predicted[file_path][question] =p
            if question and gold_sql:
                questions_gold_queries[question] = gold_sql

    return questions_gold_queries


def find_common_questions(files):
    """
    Finds questions with gold queries that appear in all three files.
    """
    common_questions = None

    for file in files:
        questions_gold_queries = extract_questions_gold_queries(file)

        if common_questions is None:
            common_questions = questions_gold_queries
        else:
            common_questions = {
                q: common_questions[q] for q in common_questions
                if q in questions_gold_queries and common_questions[q] == questions_gold_queries[q]
            }

    return common_questions


# Replace these with actual file paths
file_1 = f"outputs/experiments/errorAnalysis/{level}_llm_based_dail_res.txt"
file_2 = f"outputs/experiments/errorAnalysis/{level}_llm_based_din_res.txt"
file_3 = f"outputs/experiments/errorAnalysis/{level}_llm_based_gpt4_turbo_res.txt"

# Get common questions and their gold queries
common_q_g = find_common_questions([file_1, file_2, file_3])
output_file= f"outputs/experiments/errorAnalysis/common_{level}.txt"
qs = []
j=1
# Print results
with open(output_file, 'w', encoding='utf-8') as file:
    for question, gold_sql in common_q_g.items():
        qs.append(question)
        print(f"Question: {question}\nGold SQL: {gold_sql}\n{'-'*80}")
        file.write(f"{j}: Question: {question}\nGold SQL: {gold_sql}\n{'-'*80}\n")
        j+=1

print(len(common_q_g))
print(predicted)

def find_unique_questions(files):
    """
    Finds questions with gold queries that appear in all three files.
    """
    common_questions = None


    for file in files:
        print(file)
        questions_gold_queries = extract_questions_gold_queries(file)
        i= 1
        for q in questions_gold_queries:
            if q not in qs:
                print(i ,": ", q)
                print("Gold: ",questions_gold_queries[q] )
                print("Pred: ",  predicted[file][q])
                i+=1
                print("-------------------------")
        print("______________________")


        
find_unique_questions([file_1, file_2, file_3])
