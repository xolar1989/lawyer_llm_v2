import base64

import boto3
import numpy as np
from PIL import Image
import paddleocr
from langchain_aws import BedrockLLM, ChatBedrock
from langchain_core.messages import SystemMessage, HumanMessage
from paddleocr import PaddleOCR



import pytesseract

import shutil


def encode_image_to_base64(path):
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")


image_base64 = encode_image_to_base64('lines_2/mama1.png')

image = Image.open('lines_2/rr.png')

region = 'eu-west-1'

bedrock_runtime = boto3.client('bedrock-runtime', region_name=region)

llm = BedrockLLM(
    model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    client=bedrock_runtime,
    region_name=region,
    model_kwargs={
        "inference_config": {
            "inferenceConfigurationArn": "arn:aws:bedrock:eu-west-1:767427092061:inference-profile/eu.anthropic.claude-sonnet-4-20250514-v1:0"
        },
        "max_tokens": 40000,  # adjust as needed (max output length)
        "temperature": 0.0,  # disables randomness (deterministic output)
        "top_k": 1,  # consider only the top choice
        "top_p": 1.0,  # no nucleus sampling (with temperature 0, this has no effect)
        "stop_sequences": []  # leave empty unless you're inserting your own delimiters
    }
)


llm_v2 = ChatBedrock(
    model_id="arn:aws:bedrock:eu-west-1:767427092061:inference-profile/eu.anthropic.claude-sonnet-4-20250514-v1:0",
    provider="anthropic",
    client=bedrock_runtime,
    region_name=region,
    model_kwargs={
        "max_tokens": 40000,  # adjust as needed (max output length)
        "temperature": 0.0,  # disables randomness (deterministic output)
        "top_k": 1,  # consider only the top choice
        "top_p": 1.0,  # no nucleus sampling (with temperature 0, this has no effect)
        "stop_sequences": []  # leave empty unless you're inserting your own delimiters
    }
    # other params...
)

system_message = SystemMessage(
    content="You are a helpful assistant that extract text from image"
)

human_message = HumanMessage(
    content=[
        {
            "type": "text",
            "text": f"""
                    
Extract the visible text from the provided input exactly as it appears. Follow these specific guidelines:

1. Preserve all **newlines**, **line breaks**, **paragraph spacing**, and **indentations**. The structure of the text must match the original layout.
2. Do not remove or reinterpret **superscript** or **subscript** characters. Keep them as they visually appear using their appropriate Unicode representations, if available.
3. If a word is **split with a hyphen at the end of a line**, retain the hyphen and the line break exactly as shown.
   - Do not attempt to merge hyphenated words across lines.
4. Do not apply any formatting, LaTeX, or Markdown. Return the content as raw **plain text**.
5. Return the content as raw plain text — no summaries, no interpretation, no formatting, no cleaning.
6. Do not remove or change any visible character. The output must be byte-for-byte identical to the input.


"""
        },
        {
            "type": "image",
            "source": {"type": "base64", "media_type": "image/png", "data": image_base64}
        },
    ]
)

ww = llm_v2.invoke(
    [system_message, human_message]
)

print(shutil.which("tesseract"))
r = shutil.which("tesseract")

image_np = np.array(image)

ocr = PaddleOCR(lang='en')
print("PaddleOCR ready")

result = ocr.ocr('lines_2/rr.png', det=False, cls=False)

text = pytesseract.image_to_string(image, lang='pol')

r = 4
