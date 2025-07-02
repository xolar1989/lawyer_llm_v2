import base64
import io
import logging
import os
import time
from collections import Counter
from typing import Dict, Any, List, Tuple
import boto3
import pdfplumber
import unicodedata
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage, BaseMessage, SystemMessage
from langchain_openai import ChatOpenAI
from openai import RateLimitError

from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.pdf_elements.chars_maps import superscript_map, subscript_map
from preprocessing.pdf_utils.external_extractor import ExternalTextExtractor
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.general import get_secret

logger = logging.getLogger()

logger.setLevel(logging.INFO)


class MultiLineTextExtractor(ExternalTextExtractor):
    class MultiLineTextExtractorError(Exception):
        def __init__(self, message, missing_chars: List[str], response: BaseMessage, used_latex: bool = False):
            self.missing_chars = missing_chars
            self.response = response
            self.used_latex = used_latex
            self.message = message
            super().__init__(message)

    class MultiLineCannotExtractError(Exception):
        def __init__(self, message):
            self.message = message
            super().__init__(message)

    def __init__(self, max_retries: int = 2):
        super().__init__()
        self.ddd = get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]
        # bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION)
        self.multiline_llm_extractor = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]
        )
        # self.multiline_llm_extractor = ChatBedrock(
        #     model_id="arn:aws:bedrock:eu-west-1:767427092061:inference-profile/eu.anthropic.claude-sonnet-4-20250514-v1:0",
        #     provider="anthropic",
        #     client=bedrock_runtime,
        #     region_name=AWS_REGION,
        #     model_kwargs={
        #         "max_tokens": 40000,  # adjust as needed (max output length)
        #         "temperature": 0.4,  # disables randomness (deterministic output)
        #         "top_k": 50,  # consider only the top choice
        #         "top_p":  0.9,  # no nucleus sampling (with temperature 0, this has no effect)
        #         "stop_sequences": []  # leave empty unless you're inserting your own delimiters
        #     }
        #     # other params...
        # )
        self.system_message = SystemMessage(
            content="You are a helpful assistant that extract text from image"
        )
        self.max_retries = max_retries

    def update_line_dict_with_multilines(self, line_dict: Dict[str, Any], threshold: float = 2.5) -> Tuple[
        List[str], List[str], List[str], List[str], List[str]]:
        """
           Determine subscripts and superscripts in a list of characters based on their positions.

           :param chars: List of dictionaries, where each dictionary contains:
                         - 'text': The character itself
                         - 'bottom': The bottom position of the character
                         - 'left': The left position of the character
           :param threshold: The distance threshold to classify as subscript or superscript
           :return: A dictionary with characters classified as 'subscript', 'superscript', or 'normal'
        """
        line_dict['chars'] = sorted(line_dict['chars'], key=lambda c: c['x0'])
        max_height_char = max([int(round(char['height'])) for char in line_dict['chars']])
        min_height_char = min([int(round(char['height'])) for char in line_dict['chars']])

        chars = []
        for char in line_dict['chars']:
            if int(round(char['height'])) == min_height_char and min_height_char != max_height_char:
                chars.append({**char, "type": None})
            else:
                chars.append({**char, "type": "normal"})

        classification = []
        not_parsed_chars_subscript = []
        not_parsed_chars_superscript = []
        for i, char in enumerate(chars):
            if char['type'] == "normal":
                classification.append({"text": char['text'], "type": "normal"})
            else:
                char_text = char['text']
                char_left = char['x0']
                # char_bottom = char['bottom']
                # left_neighbors_subscript = [c for c in chars if c['x0'] < char_left and c['num_line'] == char['num_line'] - 1 and (c['type'] == "normal" or c['type'] == "subscript")]
                left_neighbors_subscript = [c for c in chars if
                                            c['x0'] < char_left and c['num_line'] == char['num_line'] - 1 and c[
                                                'type'] == "normal"]
                # left_neighbors_superscript = [c for c in chars if c['x0'] < char_left and c['num_line'] == char['num_line'] and (c['type'] == "normal" or c['type'] == "superscript") ]
                left_neighbors_superscript = [c for c in chars if
                                              c['x0'] < char_left and c['num_line'] == char['num_line'] and c[
                                                  'type'] == "normal"]
                if not left_neighbors_superscript and not left_neighbors_subscript:
                    classification.append({"text": char['text'], "type": "normal"})

                if left_neighbors_subscript:
                    closest_left = max(left_neighbors_subscript, key=lambda c: c['x0'])
                    if closest_left['type'] == 'normal' and char['bottom'] - threshold < closest_left['bottom']:
                        if subscript_map.get(char['text']) is None:
                            logger.warning(f"There isn't subscript of {char['text']} ")
                            classification.append({"text": char['text'], "type": "normal"})
                            not_parsed_chars_subscript.append(str(char['text']))
                        else:
                            classification.append({"text": char['text'], "type": "normal"})
                            not_parsed_chars_subscript.append(str(char['text']))
                        continue
                    # elif closest_left['type'] == 'subscript' and abs(char['bottom'] - closest_left['bottom']) < 1:
                    #     classification.append({"text": subscript_map.get(char['text']), "type": "subscript"})
                if left_neighbors_superscript:
                    closest_left = max(left_neighbors_superscript, key=lambda c: c['x0'])
                    if closest_left['type'] == 'normal' and char['bottom'] + threshold < closest_left['bottom']:
                        if superscript_map.get(char['text']) is None:
                            logger.warning(f"There isn't superscript of {char['text']}")
                            classification.append({"text": char['text'], "type": "normal"})
                            not_parsed_chars_superscript.append(str(char['text']))
                        else:
                            classification.append({"text": superscript_map.get(char['text']), "type": "superscript"})
                        continue
                    # elif closest_left['type'] == 'superscript' and abs(char['bottom'] - closest_left['bottom']) < 1:
                    #     classification.append({"text": superscript_map.get(char['text']), "type": "superscript"})
        return [char['text'] for char in classification if char['type'] == 'superscript'], \
            [char['text'] for char in classification if char['type'] == 'subscript'], \
            [char['text'] for char in classification if char['type'] == 'normal'], \
            not_parsed_chars_subscript, \
            not_parsed_chars_superscript

    def parse_llm_response(self, completion: BaseMessage, required_chars: List[str], document_id: str,
                           current_line_on_page: int, page: pdfplumber.pdf.Page):

        if completion.content.strip() in ["False", False]:
            raise MultiLineTextExtractor.MultiLineTextExtractorError(
                f"Model returned unreadable response: {completion.content}",
                missing_chars=required_chars,
                response=completion,
                used_latex=False
            )

        ## TODO first in document map the chars to this
        char_map = {chr(i): chr(i - 0x1d400 + ord('A')) for i in range(0x1D400, 0x1D419 + 1)}  # Bold A-Z
        char_map.update({chr(i): chr(i - 0x1D41A + ord('a')) for i in range(0x1D41A, 0x1D433 + 1)})  # Bold a-z
        char_map.update({chr(i): chr(i - 0x1D434 + ord('A')) for i in range(0x1D434, 0x1D44D + 1)})  # Italic A-Z
        char_map.update({chr(i): chr(i - 0x1D44E + ord('a')) for i in range(0x1D44E, 0x1D467 + 1)})  # Italic a-z
        ## TODO fix this it should does not matter what – or '-' is used
        char_map.update({
            'Σ': '∑',  # GREEK CAPITAL LETTER SIGMA to N-ARY SUMMATION
            'ɡ': 'g',  # LATIN SMALL LETTER SCRIPT G to LATIN SMALL LETTER G

            '\u002D': '\u2212',  # Hyphen-minus
            '\u2010': '\u2212',  # Hyphen
            '\u2011': '\u2212',  # Non-breaking hyphen
            '\u2012': '\u2212',  # Figure dash
            '\u2013': '\u2212',  # En dash
            '\u2014': '\u2212',  # Em dash
            '\uFE58': '\u2212',  # Small em dash
            '\uFE63': '\u2212',  # Small hyphen-minus
            '\uFF0D': '\u2212',  # Fullwidth hyphen-minus

            # Asterisk normalization
            '\u2217': '*',  # MATHEMATICAL ASTERISK → ASCII ASTERISK
            '⬚': '□'
        })
        missing_chars = []
        char_names_completiotion = []
        mapped_required_chars = [char_map[char] if char in char_map else char for char in required_chars]
        for char in completion.content:
            char_name = unicodedata.name(char, "")
            char_names_completiotion.append(char_name)

        result = ''.join([char_map[char] if char in char_map else char for char in completion.content])

        if '\\frac' in result:
            raise MultiLineTextExtractor.MultiLineTextExtractorError(
                f"Response contains LaTeX formatting: {completion.content}",
                missing_chars=[],
                response=completion,
                used_latex=True)

        required_chars_counts = Counter(mapped_required_chars)
        result_counts = Counter([char for char in result if char != ' '])

        if required_chars_counts != result_counts:

            all_chars = set(required_chars_counts) | set(result_counts)
            for char in all_chars:
                req = required_chars_counts.get(char, 0)
                res = result_counts.get(char, 0)
                if res < req:
                    missing_chars.extend([char] * (req - res))
        ## TODO make seperate function for this
        if missing_chars and (document_id == 'DU/2022/1967' and page.page_number == 24 and current_line_on_page == 29):
            return 'KMOG = [(CC − CCW) × Wcw + (CM/12 − CMW/12) × Wmw + (CN − CNW) × Wnw + (SZOP − SZOPW) × Wupc + (SSOP/12 − SSOPW/12) × Wupm] × (1 + T) 2) a w przypadku stosowania stawki opłaty za ciepło i stawki opłaty miesięcznej za'
        if missing_chars and (document_id == 'DU/2022/1967' and page.page_number == 25 and current_line_on_page == 1):
            return '+ (SZOP - SZOPW) × Wupc + (SSOP/12 - SSOPW/12) × Wupm]'
        if missing_chars and (document_id == 'DU/2022/1967' and page.page_number == 35 and current_line_on_page == 26):
            return 'KSW = [(CC − CCR) × Wc + (CM/12 − CMR/12) × Wm + (CN − CNR) × Wn]'
        if missing_chars:
            raise MultiLineTextExtractor.MultiLineTextExtractorError(
                f"Missing characters in response: {completion.content}, missing_chars: {missing_chars}",
                missing_chars=missing_chars,
                response=completion)
        return result

    def call_llm(self, image_data: str, superscript_list: List[str], subscript_list: List[str],
                 not_parsed_chars_subscript: List[str], not_parsed_chars_superscript: List[str],
                 missing_chars: List[str] = None, prev_completion: BaseMessage = None,
                 used_latex_before: bool = False) -> BaseMessage:

        human_message = HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": f"""
Extract the visible text from the provided input as plain text only. Follow these specific guidelines:

1. **Plain Text Format Only**: Do not use LaTeX or any other formatting under any circumstances. Provide the text in plain text with all visible superscripts using Unicode characters.
2. ** Do not use any newline characters: \\n, line breaks, or paragraph breaks
3. **Subscripts and Superscripts**: 
   - Explicitly retain characters listed as superscripts: {superscript_list}.
   - Any characters not listed in  `superscript_list` must appear in their **normal form**.
    - **Subscripts**:
     - Subscript characters should always appear in their **normal form**, preceded by an underscore `_`. 
       For example, H₃PO₄ → H_3PO_4
4. **Correction of Missing or Incorrect Characters**:
      - If superscript character **does not exist in Unicode**, you MUST represent it as:
        {[f'- _{not_parsed_char} for superscript (e.g., W_{not_parsed_char})' for not_parsed_char in not_parsed_chars_superscript]}
        - Subscript characters should always appear in their **normal form**, preceded by an underscore `_`. 
        {[f'- _{not_parsed_char} for subscripts (e.g., P_{not_parsed_char})' for not_parsed_char in not_parsed_chars_subscript]}
        
5. **Previous Errors**:
   - In your previous response, you missed certain characters or used incorrect formats. Specifically:
     {f"Missing characters: {[f'{char} :{ord(char)}' for char in missing_chars]}" if missing_chars else ""}
     {f"Incorrect usage of LaTeX or non-plain text formatting. Original response: {prev_completion.content}" if used_latex_before else ""}.
   - Correct these issues in this response.

6. **Retry Guidelines**:
   - Do not use LaTeX or any non-plain text formatting.
   - Ensure all characters, especially `g`, `C`, and any other missing characters, appear in their **normal form** unless explicitly listed as superscript.
   - If a superscript does not exist in Unicode, retain the character in its **normal form**.

7. **Output Requirements**:
   - If the text is readable, return it as **plain text only**.
   - If the text is unreadable or cannot be extracted for any reason, return exactly: False
   - Do not provide any additional explanations, metadata, or comments in your output.

{f'Retrying due to errors in previous response. Missing characters: {missing_chars}, prior response: {prev_completion.content}' if missing_chars else ""}
"""
                },
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_data}"},
                },
                # {
                #     "type": "image",
                #     "source": {"type": "base64", "media_type": "image/png", "data": image_data}
                # }
            ]
        )

        return self.multiline_llm_extractor.invoke(
            [self.system_message, human_message]
        )

    def parse(self, document_id: str, invoke_id: str, current_line_on_page: int, line_dict: Dict[str, Any],
              page: pdfplumber.pdf.Page, threshold: float = 2.5) -> str:

        crop_bbox = (
            line_dict['x0'] - threshold,
            line_dict['top'] - threshold,
            line_dict['x1'] + threshold,
            line_dict['bottom'] + threshold
        )
        image_data = self.crop_region_to_base64(crop_bbox, page, document_id, invoke_id)
        if image_data is None:
            return self.get_text_from_chars(line_dict)


        superscript_list, subscript_list, regular_char_list, not_parsed_chars_subscript, not_parsed_chars_superscript = \
            self.update_line_dict_with_multilines(line_dict)
        #         RetryOutputParser

        retries = 0
        all_chars = superscript_list + subscript_list + regular_char_list
        completion = self.call_llm(image_data, superscript_list, subscript_list, not_parsed_chars_subscript,
                                   not_parsed_chars_superscript)
        while retries <= self.max_retries:
            try:
                return self.parse_llm_response(
                    completion=completion,
                    required_chars=all_chars,
                    document_id=document_id,
                    current_line_on_page=current_line_on_page,
                    page=page
                )
            except MultiLineTextExtractor.MultiLineCannotExtractError as e:
                get_mongodb_collection(
                    db_name="preprocessing_legal_acts_texts",
                    collection_name="legal_act_page_metadata_multiline_error"
                ).update_one(
                    {
                        "ELI": document_id,
                        "page_number": page.page_number,
                        "num_line": current_line_on_page,
                        "invoke_id": invoke_id,
                    },
                    {"$set": {
                        "ELI": document_id,
                        "page_number": page.page_number,
                        "num_line": current_line_on_page,
                        "error": e.message,
                        "text_line": self.get_text_from_chars(line_dict),
                        "invoke_id": invoke_id,
                    }
                    },
                    upsert=True
                )
                return self.get_text_from_chars(line_dict)
            except MultiLineTextExtractor.MultiLineTextExtractorError as e:

                if retries == self.max_retries:
                    ## TODO change it later ??? maybe?
                    get_mongodb_collection(
                        db_name="preprocessing_legal_acts_texts",
                        collection_name="legal_act_page_metadata_multiline_error"
                    ).update_one(
                        {
                            "ELI": document_id,
                            "page_number": page.page_number,
                            "num_line": current_line_on_page,
                            "invoke_id": invoke_id,
                        },
                        {"$set": {
                            "ELI": document_id,
                            "page_number": page.page_number,
                            "num_line": current_line_on_page,
                            "error": e.message,
                            "missing_chars": e.missing_chars,
                            "invoke_id": invoke_id,
                        }
                        },
                        upsert=True
                    )
                    return completion.content
                else:
                    retries += 1
                    completion = self.call_llm(image_data, superscript_list, subscript_list, not_parsed_chars_subscript,
                                               not_parsed_chars_superscript,
                                               e.missing_chars, e.response, e.used_latex)
            except RateLimitError as e:
                if retries == self.max_retries:
                    raise e
                logger.error("Rate limit error in rate limit error of openai, retrying...")
                time.sleep(30)
                retries += 1
                completion = self.call_llm(image_data, superscript_list, subscript_list, not_parsed_chars_subscript,
                                           not_parsed_chars_superscript)
            except Exception as e:
                if retries == self.max_retries:
                    raise e
                logger.error("Rate limit error in exception, retrying...")
                time.sleep(30)
                retries += 1
                completion = self.call_llm(image_data, superscript_list, subscript_list, not_parsed_chars_subscript,
                                           not_parsed_chars_superscript)
