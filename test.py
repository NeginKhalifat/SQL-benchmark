
# from openai import OpenAI
# model = "meta-llama/Meta-Llama-3.1-70B-Instruct"

# client = OpenAI(
#         api_key="token-wdmuofa",
#         base_url="http://anagram.cs.ualberta.ca:2000/v1" # Choose one from the table
#         # base_url = "http://turin4.cs.ualberta.ca:2001/v1"
        
#     )
     
  
# response = client.chat.completions.create(
#         model=model,
#         messages="hi",
#         max_tokens=512,
#     )
# print(response)
# import os
# import pandas as pd
# count =0
# directory = "data/processed/synthetic_queries/schema_guided_llm_refinement/correct_sqls"
# all =0
# for filename in os.listdir(directory):
#     if filename.endswith(".csv"):
#         count+=1
#         df = pd.read_csv(directory +"/"+filename)
#         all+=len(df)

# print(count)
# print(all)
# import matplotlib.pyplot as plt
# import numpy as np

# # Data
# metrics = ['Easy', 'Medium', 'Hard', 'Extra', 'Table 1', 'Table 2', 'Table 3', 'Nested', 'Total']
# llm_based = [1.70, 4.90, 5.00, 10.65, 9.20, 9.55, 3.10, 6.55, 22.25]
# schema_guided = [1.45, 7.75, 5.40, 12.90, 15.10, 8.70, 2.75, 7.85, 27.50]
# schema_guided_llm = [1.90, 7.40, 5.20, 10.50, 15.70, 6.15, 2.50, 6.30, 25.00]

# x = np.arange(len(metrics))
# width = 0.25

# # Plot
# plt.figure(figsize=(12, 6))
# plt.bar(x - width, llm_based, width, label='LLM-Based', color='skyblue')
# plt.bar(x, schema_guided, width, label='Schema-Guided', color='lightgreen')
# plt.bar(x + width, schema_guided_llm, width, label='Schema-Guided + LLM Refinement', color='salmon')

# # Labels and title
# plt.xlabel('Metrics')
# plt.ylabel('Average Count per Database')
# plt.title('Comparison of SQL Synthesis Methods')
# plt.xticks(x, metrics, rotation=45)
# plt.legend()
# plt.tight_layout()
# plt.show()
from openai import OpenAI
openai_api_key = "EMPTY"
openai_api_base = "http://anagram.cs.ualberta.ca:8000/v1"

client = OpenAI(
    api_key=openai_api_key,
    base_url=openai_api_base,
)

MODEL = "meta-llama/Meta-Llama-3.1-70B"
messages=[
                {
                    "role": "system",
                    "content": f"""hi, how are you?"""}]
response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        max_tokens=512,
 
    )
print(response)