med_common_q = ["What are the 5 most common template type descriptions with codes not equal to 'XXX'?", 'What are the names of employees and the number of times they were hired before 2020?', 'What is the total vendor requested price of the properties where the agreed selling price is higher than the vendor requested price?', "What is the content and average id of TV channels with series name 'Series_1' and id between 1 and 100?", 'What are the names, distinct number of friends and grades of high schoolers in grade 10 or above?', 'What courses are not taught by the teacher with ID 1?', 'What are the names of the 11th to 20th conductors between the ages of 40 and 50?', 'What are the nationalities of conductors aged 60 or under, listed in order of their total age from lowest to highest?', 'For each department with an id less than or equal to 10, what is the highest degree program id?', 'What are the total number of museums and total staff count for museums ranked 11th to 20th?', 'What is the name of each museum and the total number of tickets sold for each museum?', 'What are the names of the 11th to 20th museums visited by visitors with a membership level higher than 2, listed alphabetically?', 'What is the highest number of cylinders in cars, considering only cars with at least 4 cylinders and ranking the results in descending order by the number of cylinders?', 'What are the counts of distinct battle names, distinct latin commanders, and total battles, for battles 6 through 10?', 'What are the last 10 singer ids in descending order, excluding the first 10?', 'What are the names of singers with a net worth of more than 10 million?', 'What are the first and last names of students under the age of 20, listed from the 6th to the 10th student?', 'What are the average flight numbers for each airline?', 'What are the next 5 area codes (after the first 5) that are 500 or higher?']
hard_common_q = ["What are the last 10 template type descriptions in descending order, excluding the first 10, for template type codes between 'A' and 'Z'?", 'What are the names of the 11th to 20th oldest employees who are 30 years old or younger?', 'What are the names of the 11th to 20th youngest employees who are not 25 years old?', 'What is the average number of viewers for each series of TV channels, excluding BBC?', 'What are the names of the high school students in grades 9-12 and how many unique people do they like?', 'What are the names of the 6th to 10th 9th graders in high school along with their friend IDs?', 'What are the counts of stadiums and distinct concerts where singers with ID 10 or less performed, excluding the first 5 results?', 'What is the number of unique poker players of each nationality, for people with a height of 175, listed in ascending order by nationality?', 'What are the names of singers with a net worth of 50 million or less, along with their total song sales, listed in descending order of total sales?', 'What are the names of singers with a net worth of at least 10 million and their total song sales, listed in ascending order by singer name?', 'How many distinct states have votes with area codes between 200 and 300, made in the last 30 days?', 'What are the contestant numbers and the number of votes for the 11th to 20th most voted contestants?']
def extract_questions_gold_queries(file_path):
    """
    Extracts questions and their corresponding gold queries from the given file.
    """
    questions_gold_queries = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read().split("--------------------------------------------------------------------------------")  # Separate each block

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
                    pred = line.replace("Predicted SQL:", "").strip()

            if question and gold_sql:
                if question not in med_common_q:
                    questions_gold_queries[question] = gold_sql
                    print("Question: ", question)
                    print("Gold Sql: ", gold_sql)
                    print("pred",pred)
                    print("-----------------------------")

    return questions_gold_queries
file_1 = "outputs/experiments/errorAnalysis/Medium_llm_based_dail_res.txt"
file_2 = "outputs/experiments/errorAnalysis/Medium_llm_based_din_res.txt"
file_3 = "outputs/experiments/errorAnalysis/Medium_llm_based_gpt4_turbo_res.txt"

extract_questions_gold_queries(file_2)
