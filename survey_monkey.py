import requests
import pandas as pd
import json
import random

# Replace with your actual access token
ACCESS_TOKEN = 'rSiHfXaQOLwrqr4TrWy5sPVzE-n0Gh051HwjVJOGGOpTSZocz7KcjsE2vRysQ7s4A8e3Ob4ASz2VBN98WvV6JiJTb1l7vcD6TM6HsHLOl9UlglVwR5hbJZow8-w-dqaR'
sql_file = "outputs/experiments/SQL2text/llm_based/evaluation_subsets/student_query_assignments_optimized.csv"
BASE_URL = 'https://api.surveymonkey.com/v3'

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def create_survey(title):
    """
    Create a new survey with the given title.
    """
    url = f'{BASE_URL}/surveys'
    data = {'title': title}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 201:
        survey = response.json()
        print(f"Survey '{title}' created successfully with ID: {survey['id']}")
        return survey['id']
    else:
        print(f"Failed to create survey: {response.status_code}")
        print(response.json())
        return None

def add_page_to_survey(survey_id, title, description):
    """
    Add a new page to the specified survey.
    """
    url = f'{BASE_URL}/surveys/{survey_id}/pages'
    data = {'title': title, 'description': description}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 201:
        page = response.json()
        print(f"Page '{title}' added successfully with ID: {page['id']}")
        return page['id']
    else:
        print(f"Failed to add page: {response.status_code}")
        print(response.json())
        return None

def add_question_to_page(survey_id, page_id, question_text, is_open_ended=False, choices=None):
    """
    Add a question to a specific page in the survey.
    
    Parameters:
        - survey_id: ID of the survey.
        - page_id: ID of the page within the survey.
        - question_text: The main text of the question.
        - is_open_ended: Boolean indicating if the question is open-ended.
        - choices: List of choices for multiple-choice questions. If None, defaults to Yes/No.
    """
    url = f'{BASE_URL}/surveys/{survey_id}/pages/{page_id}/questions'
    
    # Prepare the data payload based on question type
    question_data = {
        'headings': [{'heading': question_text}],
    }
    
    if is_open_ended:
        question_data.update({
            'family': 'open_ended',
            'subtype': 'single',
            'answers': {}
        })
    else:
        if choices is None:
            # Default to Yes/No choices
            choices = [
                {'text': 'Yes'},
                {'text': 'No'}
            ]
        question_data.update({
            'family': 'single_choice',
            'subtype': 'vertical',
            'answers': {
                'choices': choices
            }
        })
    
    response = requests.post(url, headers=headers, data=json.dumps(question_data))
    
    if response.status_code == 201:
        question = response.json()
        print(f"Question added successfully with ID: {question['id']}")
        return question['id']
    else:
        print(f"Failed to add question: {response.status_code}")
        print(response.json())
        return None

def generate_schema_description(db_name):
    """
    Generate a dummy schema description for the given database name.
    Replace this with actual schema information as needed.
    """
    schema_description = f"### Database Schema for {db_name}\n\n"
    schema_description += "table: example_table\n\n"
    schema_description += "columns:\n"
    schema_description += "- column1\n"
    schema_description += "- column2\n"
    schema_description += "- column3\n\n"
    schema_description += "primary key: column1\n\n"
    schema_description += "--------------------------------------------------------\n"
    return schema_description

def get_predefined_questions():
    """
    Return a list of predefined regular questions with their descriptions.
    """
    return [
        ("Does the query execute without errors?", "Ensure the query runs successfully without syntax or logical issues."),
        ("Are table and column names descriptive?", "Good readability relies on clear and self-explanatory names."),
        ("Is the query structured logically and easy to follow?", "Ensure proper organization in WHERE, GROUP BY, and ORDER BY clauses."),
        ("Are conditions simplified without redundant logic?", "Avoid unnecessary conditions or repetition."),
        ("Do filters avoid transformations that hinder indexing?", "Filters should use direct comparisons to leverage indexing."),
        ("Are unnecessary joins or tables avoided?", "Include only essential tables in the query."),
        ("Are indexed columns used effectively?", "Use indexed columns for joins, filters, and sorting."),
        ("Are all non-aggregated columns in SELECT included in GROUP BY?", "Ensure logical correctness by grouping all non-aggregated columns."),
        ("Are there any unusual patterns or areas for improvement?", "Document observations about edge cases or optimizations."),
        ("Please convert the SQL query to a natural language question.", "Provide a natural language equivalent of the SQL query.")
    ]

validity_questions = [
    {
        "query": "SELECT * FROM employees;",
        "question": "What does this query retrieve from the employees table? (Disregard the provided schema above.)",
        "options": [
            {"text": "A) All columns and rows"},
            {"text": "B) Only specific columns"},
            {"text": "C) Only specific rows"},
            {"text": "D) None of the above"}
        ],
        "answer": "A"
    },
    {
        "query": "SELECT COUNT(*) FROM table;",
        "question": "What is the purpose of using COUNT(*) in this query? (Assume no prior schema knowledge.)",
        "options": [
            {"text": "A) To count all columns"},
            {"text": "B) To count all rows"},
            {"text": "C) To count unique rows"},
            {"text": "D) To count specific rows"}
        ],
        "answer": "B"
    },
    {
        "query": "SELECT name FROM orders WHERE quantity < 0;",
        "question": "Would this query retrieve any rows from the orders table? (Ignore the context of the schema above.)",
        "options": [
            {"text": "Yes"},
            {"text": "No"}
        ],
        "answer": "No"
    },
    {
        "query": "SELECT 1;",
        "question": "Is this query valid SQL? (Focus solely on the query itself.)",
        "options": [
            {"text": "Yes"},
            {"text": "No"}
        ],
        "answer": "Yes"
    },
    {
        "query": "SELECT DISTINCT category FROM products;",
        "question": "What result would this query produce when executed on the products table? (No schema reference needed.)",
        "options": [
            {"text": "A) All categories"},
            {"text": "B) Unique categories"},
            {"text": "C) All rows"},
            {"text": "D) None of the above"}
        ],
        "answer": "B"
    },
    {
        "query": "SELECT MAX(price) FROM items;",
        "question": "What does this query return from the items table? (Schema details are not required.)",
        "options": [
            {"text": "A) The minimum price"},
            {"text": "B) The average price"},
            {"text": "C) The maximum price"},
            {"text": "D) None of the above"}
        ],
        "answer": "C"
    },
    {
        "query": "SELECT id, name FROM students WHERE age < 18;",
        "question": "Does this query retrieve rows for students under 18? (Do not consider the database structure above.)",
        "options": [
            {"text": "Yes"},
            {"text": "No"}
        ],
        "answer": "Yes"
    },
    {
        "query": "SELECT email FROM users WHERE email LIKE '%@gmail.com';",
        "question": "What is the purpose of the condition used in this query? (No schema assumptions are necessary.)",
        "options": [
            {"text": "A) To retrieve all email addresses"},
            {"text": "B) To retrieve email addresses ending with '@gmail.com'"},
            {"text": "C) To retrieve email addresses starting with '@gmail.com'"},
            {"text": "D) None of the above"}
        ],
        "answer": "B"
    },
    {
        "query": "SELECT SUM(amount) FROM transactions;",
        "question": "What does this query calculate using the transactions table? (Ignore any provided schema above.)",
        "options": [
            {"text": "A) The total sum of the 'amount' column"},
            {"text": "B) The average of the 'amount' column"},
            {"text": "C) The maximum value in the 'amount' column"},
            {"text": "D) None of the above"}
        ],
        "answer": "A"
    },
    {
        "query": "SELECT id FROM orders WHERE date > '2025-01-01';",
        "question": "Would this query retrieve IDs of orders placed after January 1, 2025? (Schema details can be disregarded.)",
        "options": [
            {"text": "Yes"},
            {"text": "No"}
        ],
        "answer": "Yes"
    },
    {
        "query": None,
        "question": "What does the SELECT keyword do in SQL?",
        "options": [
            {"text": "A) Retrieves data from a table"},
            {"text": "B) Deletes data from a table"},
            {"text": "C) Updates data in a table"},
            {"text": "D) None of the above"}
        ],
        "answer": "A"
    },
    {
        "query": None,
        "question": "What is the purpose of the WHERE clause in an SQL query?",
        "options": [
            {"text": "A) To filter rows based on a condition"},
            {"text": "B) To join tables"},
            {"text": "C) To group rows"},
            {"text": "D) None of the above"}
        ],
        "answer": "A"
    },
]

def main():
    # Load CSV data
    csv_file_path = sql_file
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Loaded CSV data from {csv_file_path}")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # Filter queries for student_id = 1
    student_id = 1  # You can parameterize this as needed
    student_df = df[df['student_id'] == student_id]
    if student_df.empty:
        print(f"No data found for student_id = {student_id}")
        return
    else:
        print(f"Filtered data for student_id = {student_id}")

    # Create survey
    survey_title = 'SQL Query Evaluation Survey'
    survey_id = create_survey(survey_title)
    if not survey_id:
        print("Survey creation failed. Exiting.")
        return

    # Prepare predefined and validity questions
    regular_questions = get_predefined_questions()
    validity_questions_pool = validity_questions.copy()
    random.shuffle(validity_questions_pool)  # Shuffle to randomize validity question insertion

    insertion_interval = 10  # Insert a validity question every 10 regular questions
    current_regular_question_count = 0

    for db_name, db_group in student_df.groupby('db_name'):
        print(f"Processing database: {db_name}")
        schema_info = generate_schema_description(db_name)
        page_title = f"Database: {db_name}"
        page_description = schema_info
        page_id = add_page_to_survey(survey_id, page_title, page_description)
        if not page_id:
            print(f"Skipping database '{db_name}' due to page creation failure.")
            continue

        # Collect all regular questions for this page
        all_regular_questions = []
        for _, row in db_group.iterrows():
            sql_query = row.get('query', '').strip()
            if not sql_query:
                print("Empty SQL query found. Skipping.")
                continue

            for question, description in regular_questions:
                question_entry = {
                    'type': 'regular',
                    'sql_query': sql_query,
                    'question': question,
                    'description': description
                }
                all_regular_questions.append(question_entry)

        total_regular_questions = len(all_regular_questions)
        print(f"Total regular questions for '{db_name}': {total_regular_questions}")

        # Process regular questions in blocks of 10
        for i in range(0, total_regular_questions, insertion_interval):
            block = all_regular_questions[i:i + insertion_interval]
            if not block:
                continue

            # Determine if there are enough validity questions left
            if not validity_questions_pool:
                print("No more validity questions available to insert.")
                # Optionally, you can reshuffle or reuse validity questions here
                break  # Exiting if no validity questions are left

            # Select a validity question
            v_question = validity_questions_pool.pop(0)

            # Choose a random position to insert the validity question within the block
            insert_position = random.randint(0, len(block))  # Position can be at the end

            # Create a validity question entry
            v_question_entry = {
                'type': 'validity',
                'query': v_question.get('query'),
                'question': v_question.get('question'),
                'options': v_question.get('options'),
                'answer': v_question.get('answer')
            }

            # Insert the validity question into the block
            block.insert(insert_position, v_question_entry)

            # Add all questions in the block to the survey page
            for q in block:
                if q['type'] == 'regular':
                    # Format the regular question
                    if q['question'].startswith("Please convert"):
                        is_open_ended = True
                    else:
                        is_open_ended = False

                    question_text = f"**Query:** {q['sql_query']}\n\n{q['question']}"
                    add_question_to_page(
                        survey_id, page_id, question_text, is_open_ended=is_open_ended
                    )
                    current_regular_question_count += 1
                elif q['type'] == 'validity':
                    # Format the validity question
                    if q['query']:
                        question_text = f"**Query:** {q['query']}\n\n{q['question']}"
                    else:
                        question_text = f"{q['question']}"
                    
                    # Determine if the validity question is multiple-choice or Yes/No
                    if len(q['options']) > 2:
                        choices = q['options']
                    else:
                        # Assume Yes/No if only two options are provided
                        choices = q['options']

                    add_question_to_page(
                        survey_id, page_id, question_text, is_open_ended=False, choices=choices
                    )
            

        print(f"Completed processing for database: {db_name}")
    add_question_to_page(
                        survey_id, page_id, "What is your code?", is_open_ended=True
                    )
    print("Survey creation and question addition completed successfully.")

if __name__ == '__main__':
    main()
