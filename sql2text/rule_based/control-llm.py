from openai import OpenAI
from pydantic import BaseModel

import guidance
import outlines
import transformers
from openai import AsyncOpenAI
import tiktoken
from guidance import models, gen
from outlines.models.openai import OpenAI, OpenAIConfig

# lm = guidance.models.Model(
#     "http://anagram.cs.ualberta.ca:8000",
#     api_key="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",
# )
# lm + "Tell me a joke." + guidance.gen("simple_joke", max_tokens=20)

# print(str(lm))

# Define a Guidance program for a chatbot

# client = OpenAI(
#     api_key="EMPTY",
#     base_url="http://anagram.cs.ualberta.ca:8000/v1",
# )

from guidance import models, gen

# This relies on the environment variable TOGETHERAI_API_KEY being set


model = "meta-llama/Meta-Llama-3-70B-Instruct"
config = transformers.AutoConfig.from_pretrained(model, trust_remote_code=True)
tokenizer = transformers.AutoTokenizer.from_pretrained(model, trust_remote_code=True)

# lm = models.TogetherAI(
#     model,
#     base_url="http://anagram.cs.ualberta.ca:8000/v1",
#     api_key="EMPTY",
#     tokenizer=tokenizer,
# )


# stop_tokens = [",", "}", "\n"]
# temperature = 0.0

# lm += f"""The most famous piece of japanese literature in a JSON format is:
# {{
#     "title_english": {gen(name='title_english', temperature=temperature, max_tokens=50, stop=stop_tokens)},
#     "title_japanese": {gen(name='title_japanese', temperature=temperature, max_tokens=50, stop=stop_tokens)},
#     "author": {gen(name='author', temperature=temperature, max_tokens=50, stop=stop_tokens)},
#     "year": {gen(name='year', temperature=temperature, max_tokens=50, stop=stop_tokens)}
# }}
# """
# print(str(lm))

config = OpenAIConfig(model=model)
client = AsyncOpenAI(base_url="http://anagram.cs.ualberta.ca:8000/v1", api_key="EMPTY")

lm = OpenAI(client, config, tokenizer)
# lm = models.OpenAI(
#     client=client,
#     tokenizer=tokenizer,
#     model=model,
# )


# print(client.models.list())


chat_completion = client.chat.completions.create(
    model="meta-llama/Meta-Llama-3-70B-Instruct",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is the purpose of life?"},
    ],
    max_tokens=4096,
    extra_body={
        "stop_token_ids": [128009, 128001],
    },
)




