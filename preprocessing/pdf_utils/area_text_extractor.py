import logging
import time
from typing import Dict, Any, List

import boto3
import pdfplumber
from langchain_aws import ChatBedrock
from langchain_core.messages import SystemMessage, HumanMessage, BaseMessage
from langchain_openai import ChatOpenAI
from openai import RateLimitError

from preprocessing.pdf_boundaries.boundaries import BoundariesArea
from preprocessing.pdf_utils.external_extractor import ExternalTextExtractor
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.general import get_secret



logger = logging.getLogger()

logger.setLevel(logging.INFO)


class PageAreaTextExtractor(ExternalTextExtractor):

    def __init__(self, max_retries: int = 3):
        super().__init__()
        self.bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION)
        self.multiline_llm_extractor = ChatBedrock(
            model_id="arn:aws:bedrock:eu-west-1:767427092061:inference-profile/eu.anthropic.claude-sonnet-4-20250514-v1:0",
            provider="anthropic",
            client=self.bedrock_runtime,
            region_name=AWS_REGION,
            model_kwargs={
                "max_tokens": 40000,  # adjust as needed (max output length)
                "temperature": 0.0,  # disables randomness (deterministic output)
                "top_k": 40,  # consider only the top choice
                "top_p": 1.0,  # no nucleus sampling (with temperature 0, this has no effect)
                "stop_sequences": []  # leave empty unless you're inserting your own delimiters
            }
            # other params...
        )
        self.system_message = SystemMessage(
            content="You are a helpful assistant that extract text from image"
        )
        self.max_retries = max_retries

    def call_llm(self, image_data: str, is_error: bool = False) -> BaseMessage:
        human_message = HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": f"""
                    {"RETRY INSTRUCTION — STRICT COMPLIANCE REQUIRED" if is_error else ""}
                    
Extract the visible text from the provided input exactly as it appears. {"The previous output was incorrect because it changed the structure or merged split words." if is_error else ""} Follow these specific guidelines:

1. Preserve all **newlines**, **line breaks**, **paragraph spacing**, and **indentations**. The structure of the text must match the original layout.
2. Do not remove or reinterpret **superscript** or **subscript** characters. Keep them as they visually appear using their appropriate Unicode representations, if available.
3. If a word is **split with a hyphen at the end of a line**, retain the hyphen and the line break exactly as shown.
   - Do not attempt to merge hyphenated words across lines.
4. Do not apply any formatting, LaTeX, or Markdown. Return the content as raw **plain text**.
5. Return the content as raw plain text — no summaries, no interpretation, no formatting, no cleaning.
6. Do not remove or change any visible character. The output must be byte-for-byte identical to the input.
{"7. This is a retry due to previous non-compliance. Ensure full fidelity this time." if is_error else ""}
"""
                },
                {
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/png", "data": image_data}
                }
            ]
        )

        return self.multiline_llm_extractor.invoke(
            [self.system_message, human_message]
        )

    @staticmethod
    def get_lines(text: str) -> List[str]:
        # Split text into physical lines
        lines = text.splitlines()

        # Clean up empty or whitespace-only lines
        visible_lines = [line.strip() for line in lines if line.strip()]

        return visible_lines

    def parse_llm_response(self, completion: BaseMessage, page: pdfplumber.pdf.Page, line_dict: List[Dict[str, Any]]):
        lines = PageAreaTextExtractor.get_lines(completion.content)
        if len(lines) != len(line_dict):
            raise Exception(
                f"Missing characters in response: {completion.content}, required len of lines: {len(line_dict)}, "
                f"gotten: {len(lines)}")
        return completion.content

    def parse(self, area: BoundariesArea, page: pdfplumber.pdf.Page, line_dict: List[Dict[str, Any]],
              document_id: str, invoke_id: str,
              threshold: float = 2.5) -> str:
        image_data = self.crop_region_to_base64(area.bbox, page, document_id, invoke_id)
        completion = self.call_llm(image_data)
        new_lines = completion.content.count('\n')

        lines = completion.content.splitlines()

        # Clean up empty or whitespace-only lines
        visible_lines = [line for line in lines if line.strip()]

        ww = len(PageAreaTextExtractor.get_lines(completion.content))
        retries = 0
        while retries <= self.max_retries:
            try:
                return self.parse_llm_response(completion, page, line_dict)
            except self.bedrock_runtime.exceptions.ThrottlingException as e:
                if retries == self.max_retries:
                    raise e
                logger.error(f"Rate limit error in rate limit throttling error of bedrock {e}, retrying...")
                time.sleep(60)
                retries += 1
                completion = self.call_llm(image_data)
            except Exception as e:
                if retries == self.max_retries:
                    raise e
                logger.error(f"Rate limit error in rate limit error of bedrock {e}, retrying...")
                time.sleep(60)
                retries += 1
                completion = self.call_llm(image_data, is_error=True)

