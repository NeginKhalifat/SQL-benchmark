from dotenv import find_dotenv, load_dotenv

_ = load_dotenv(find_dotenv())

# Set up the together.ai API key
import os
import pickle

import requests

together_api_key = os.getenv("TOGETHER_API_KEY")
headers = {
    "Authorization": f"Bearer {together_api_key}",
    "Content-Type": "application/json",
}
model = "togethercomputer/llama-2-7b-chat"

prompt = """
Please write me a birthday card for my dear friend, Andrew.
"""
prompt = f"[INST]{prompt}[/INST]"
temperature = 0.0
max_tokens = 1024
data = {
    "model": model,
    "prompt": prompt,
    "temperature": temperature,
    "max_tokens": max_tokens,
}
import json

url = "https://api.together.xyz/inference"
response = requests.post(url, headers=headers, json=data)


def string_to_dict(given_text):
    start_idx = given_text.find("{")
    end_idx = given_text.find("}") + 1
    json_text = given_text[start_idx:end_idx]

    # Remove newlines and whitespace
    json_text = json_text.replace("\n", "").strip()
    print(json_text)

    # Convert JSON text to dictionary
    try:
        revised_text = json.loads(json_text)
    except json.JSONDecodeError:
        print("Error: JSONDecodeError")
        print(given_text.split('"question":')[1])
        text = given_text.split('"question":')[1].split('"')[1]
        print("text", text)
        revised_text = {"question": text}

    # Print the dictionary
    print(revised_text)
    return revised_text


def string_to_dict(given_text, partial=False):

    given_text = given_text.replace("\t", "")
    given_text = given_text.replace("\n", "")
    given_text = given_text.replace(",}", "}")
    given_text = given_text.replace(",]", "]")
    start_idx = given_text.find("{")
    last_index_curly_brace = given_text.find("}")
    end_idx = last_index_curly_brace + 1
    json_text = given_text[start_idx:end_idx]

    # Remove newlines and whitespace
    json_text = json_text.replace("\n", "").strip()
    print("JSON TEXT")
    print(json_text)

    # Convert JSON text to dictionary
    if partial:
        try:
            revised_text = json.loads(json_text)
        except json.JSONDecodeError:
            revised_text = json_text

        # Print the dictionary
        return revised_text

    try:
        revised_text = json.loads(json_text)
    except json.JSONDecodeError:
        print("Error: JSONDecodeError")
        if '"question"' not in given_text:
            text = given_text.split("'question':")[1].split("'")[1]
            print("text", text)
        elif '"question"' in given_text:
            text = given_text.split('"question":')[1].split('"')[1]
            print("text", text)
        revised_text = {"question": text}
        print("revised_text", revised_text)

    # Print the dictionary
    return revised_text


def llama(prompt, model, temperature=0, max_tokens=1024):
    data = {
        "model": model,
        "prompt": prompt,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    response = requests.post(url, headers=headers, json=data)
    # print(response)
    try:
        return response.json()["output"]["choices"][0]["text"]
    except KeyError:
        return response.json()


def get_code_completion(
    messages, max_tokens=512, model="codellama/CodeLlama-70b-Instruct-hf"
):
    chat_completion = client.chat.completions.create(
        messages=messages,
        model=model,
        max_tokens=max_tokens,
        stop=["<step>"],
        frequency_penalty=1,
        presence_penalty=1,
        top_p=0.7,
        n=10,
        temperature=0.7,
    )

    return chat_completion


def save_checkpoint(filename, path= None):
    if path is None:
        path = "outputs/checkpoint.pkl"
    # split the filename to get the name of the file
    with open(path, "wb") as f:
        pickle.dump(filename, f)


def load_checkpoint(path = None):
    if path is None:
        path = "outputs/checkpoint.pkl"

    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None
