import os

from huggingface_hub import hf_hub_download
import re
from PIL import Image
from langchain_community.callbacks import UpstashRatelimitHandler, UpstashRatelimitError
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from openai import RateLimitError
from transformers import NougatProcessor, VisionEncoderDecoderModel
from upstash_ratelimit import FixedWindow, Ratelimit

from upstash_redis import Redis

from preprocessing.utils.general import get_secret


RateLimitError
from preprocessing.utils.llm_chain import LLMChain

w = get_secret("OpenAiApiKey", os.getenv('REGION_NAME'))["OPEN_API_KEY"]


os.environ["UPSTASH_REDIS_REST_URL"] = "https://distinct-giraffe-52426.upstash.io"
os.environ["UPSTASH_REDIS_REST_TOKEN"] = "AczKAAIjcDFkZDhhNDQxMGM4NTE0ZjBjOTIxYmRjZTU5MzIwOWRjMnAxMA"

ratelimit = Ratelimit(
    redis=Redis.from_env(),
    # 10 requests per window, where window size is 60 seconds:
    limiter=FixedWindow(max_requests=10, window=360),
)


user_id = "user_id222"  # should be a method which gets the user id
# handler = UpstashRatelimitHandler(identifier=user_id, token_ratelimit=ratelimit)
model = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    api_key=get_secret("OpenAiApiKey", os.getenv('REGION_NAME'))["OPEN_API_KEY"]
)

prompt = ChatPromptTemplate.from_template(
    "You are an expert assistant. Answer the following question:\n\n{question}"
)

# Step 3: Define the Chain
chain = prompt | model

# Step 4: Use chain.invoke() to Execute the Chain
try:
    response = chain.invoke({"question": "how is your day?"})
except UpstashRatelimitError:
    print("Handling ratelimit.", UpstashRatelimitError)

print(response)
try:
    response = chain.invoke({"question": "write huge article about diversity, it should be 2000 tokens at least?"}, config={"callbacks": [handler]})
except UpstashRatelimitError:
    print("Handling ratelimit.", UpstashRatelimitError)

print(response)
try:
    response = chain.invoke({"question": "write huge article about diversity, it should be 2000 tokens at least?"}, config={"callbacks": [handler]})
except UpstashRatelimitError as e:
    print("Handling ratelimit.", e)
# Step 5: Print the Response
print(response)

# from datasets import load_dataset
import torch
# processor = NougatProcessor.from_pretrained("facebook/nougat-base")
# model = VisionEncoderDecoderModel.from_pretrained("facebook/nougat-base")
# device = "cuda" if torch.cuda.is_available() else "cpu"
# model.to(device)
#
#
#
#
# image = Image.open("lines_2/rr.pdf").convert("RGB")
#
# images = [image]
#
#
# r = 4
#
#
# pixel_values = processor(images, return_tensors="pt")["pixel_values"]
#
# outputs = model.generate(
#     pixel_values.to(device),
#     min_length=1,
#     max_new_tokens=100,
#     bad_words_ids=[[processor.tokenizer.unk_token_id]],
# )
#
#
# sequence = processor.batch_decode(outputs, skip_special_tokens=True)[0]
# sequence = processor.post_process_generation(sequence, fix_markdown=False)
#
#
# print(repr(sequence))
# s = 4


# from pylatexenc.latex2text import LatexNodes2Text
#
#
# w  = LatexNodes2Text().latex_to_text(r"""\
# $\sum g_{2 i}$ - sumaryczną kwotę refundacji na koniec roku rozliczeniowego
# """)
#
# print(w)

print('\\frac')
def parse_to_normal(text):
    char_map = {chr(i): chr(i - 0x1d400 + ord('A')) for i in range(0x1D400, 0x1D419 + 1)}  # Bold A-Z
    char_map.update({chr(i): chr(i - 0x1D41A + ord('a')) for i in range(0x1D41A, 0x1D433 + 1)})  # Bold a-z
    char_map.update({chr(i): chr(i - 0x1D434 + ord('A')) for i in range(0x1D434, 0x1D44D + 1)})  # Italic A-Z
    char_map.update({chr(i): chr(i - 0x1D44E + ord('a')) for i in range(0x1D44E, 0x1D467 + 1)})  # Italic a-z
    char_map.update({
        'Σ': '∑', # GREEK CAPITAL LETTER SIGMA to N-ARY SUMMATION
        '-': '–', # HYPHEN-MINUS to EN DASH
        'ɡ': 'g',  # LATIN SMALL LETTER SCRIPT G to LATIN SMALL LETTER G
    })
    # Add more mappings as needed...

    return ''.join([char_map.get(char, char) for char in text])

# Example list of mathematical letters
math_letters = ['𝐾', '𝑍', '𝐾', '𝑃', '𝐺','₀', '𝑔', 'g', 'ɡ',"ł", '𝑇',  'Σ', 'ᵘ', 'ⁿ', 'ᵒ', 'ʳ', 'ᵐ']

# Parse the list into normal letters
parsed_letters = [parse_to_normal(char) for char in math_letters]
print(parsed_letters)
print([ord(char) for char in parsed_letters])
# print(parsed_letters[-1] == '∑')