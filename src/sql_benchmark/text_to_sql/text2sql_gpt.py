import glob
import json
import os
import pickle

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from langchain.chains import LLMChain

# use the load_metric function
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from tqdm import tqdm
from json_repair import repair_json
# from read_schema.read_schema import convert_json_to_schema

from sql_benchmark.query_generation.read_schema.read_schema import convert_json_to_schema

load_dotenv(find_dotenv()  )
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
from openai import OpenAI

client = OpenAI()

from tqdm import tqdm
import pandas as pd

def read_schema_pk_fk_types(db_name, file_name, all_db=None):
    """
    Read the schema, primary keys, foreign keys, and schema types for a given database.

    Args:
        db_name (str): The name of the database.
        file_name (str): The name of the file containing the schema information.

    Returns:
        tuple: A tuple containing the schema, primary keys, foreign keys, and schema types.
    """
    if all_db is None:
        all_db = convert_json_to_schema(file_name)
    schema = all_db[db_name]["schema"]
    pk = all_db[db_name]["primary_keys"]
    fk = all_db[db_name]["foreign_keys"]
    schema_types = all_db[db_name]["schema_types"]
    return schema, pk, fk, schema_types

if __name__ == "__main__":
    OUTPUT_FILE = "outputs/experiments/text2SQL_Experiment/gpt_4_turbo/llm_based_gpt_4_turbo_res.csv"
    dataset = "outputs/experiments/SyntheticBenchmarkEvaluation/combined_filtered_llm_based.json"
    test_df = pd.read_json(dataset)
    print("Number: ", len(test_df))

    # Initialize the progress bar with the total number of rows.
    pbar = tqdm(total=len(test_df), desc="Processing queries")
    # header = ['Question', 'Predicted SQL', 'Gold SQL', 'Database']
    # pd.DataFrame(columns=header).to_csv(OUTPUT_FILE, index=False, mode='w')  # Write header for CSV

    for index, row in test_df.iterrows():
        # if index<2:
        #     continue
        print(f"index is {index}")
        print(row['query'])
        print(row['question'])
        db_name = row["db_id"]
        schema, pk, fk, schema_types = read_schema_pk_fk_types(
            db_name, "test-suite-sql-eval-master/tables.json"
        )
        query = row["query"]

        pbar.update(1)

        # Make a single API request to OpenAI
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"""You are tasked with generating a correct SQL query for the following question:

Question:
{query}

Database Information:
- Schema: {schema}
- Primary Keys: {pk}
- Foreign Keys: {fk}
- Column Types: {schema_types}

Instructions (Following SQL Execution Order):

1. Understand the Question:
   - Restate the question in your own words.
   - Identify required columns, filters, aggregations, subqueries, and sorting.

2. Determine the Data Sources (FROM Clause):
   - Identify the necessary tables based on the schema.
   - Establish JOIN conditions using primary and foreign key relationships.

3. Apply Filters (WHERE Clause):
   - Determine which rows need to be filtered before any grouping or selection.

4. Plan for Subqueries:
   - Check if the question requires subqueries for nested data retrieval or intermediate calculations.
   - Decide where a subquery is needed (e.g., in the FROM clause, WHERE clause, or as a derived table).

5. Plan Aggregations (GROUP BY Clause):
   - Identify if the query needs grouping (e.g., for functions like COUNT, SUM, AVG).
   - Decide which columns need to be grouped.

6. Filter Aggregated Results (HAVING Clause):
   - If conditions must be applied on aggregated data, specify them using the HAVING clause.

7. Select the Output (SELECT Clause):
   - Determine which columns or expressions to include in the final output.
   - Ensure that selected columns align with the required grouping and calculations.

8. Sort and Limit the Results (ORDER BY and LIMIT Clauses):
   - Decide if the results need to be ordered.
   - Apply a LIMIT if only a subset of results should be returned.

9. Simulate Expert Opinions & Weighted Vote:
   - Generate three candidate SQL queries, each with an assigned confidence score.
   - Perform a weighted majority vote based on these scores to choose the final query.

10. Finalize and Validate:
    - Review the final query for correct SQL syntax and ensure it meets the question’s requirements.

11. Internal Reasoning Note:
    - Perform all intermediate reasoning steps internally to ensure the best final SQL query is produced.
    - Even if some reasoning is "lost in the middle," ensure that the final SQL query is accurate and complete.
    - Do not include these internal steps in your final output.

12. Output the Final Query:
    - Return only a JSON object with a single key "predicted_sql" containing your final SQL query.
    - Example:
      {{
        "predicted_sql": "SELECT ... FROM ... WHERE ... [subquery] ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ..."
      }}

Let’s think step by step and follow the SQL execution order to construct the best query possible.
"""
                }
            ],
            temperature=0,
            max_tokens=1000
        )

        response_text = response.choices[0].message.content.strip()
        response_json = repair_json(response_text, return_objects=True)
        predicted_query = response_json["predicted_sql"]

        print("PREDICTED: ",predicted_query)
        
        new_row = pd.DataFrame({
            'Question': [row['question']],
            'Predicted SQL': [predicted_query],
            'Gold SQL': [row['query']],
            'Database': [row['db_id']]
        })

        # Append the new row to the CSV file immediately.
        new_row.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)
        # break
    
    # Close the progress bar when done.
    pbar.close()
