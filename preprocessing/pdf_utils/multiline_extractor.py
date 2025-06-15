import base64
import io
import logging
import os
import time
from typing import Dict, Any, List, Tuple

import pdfplumber
import unicodedata
from langchain_core.messages import HumanMessage, BaseMessage, SystemMessage
from langchain_openai import ChatOpenAI
from openai import RateLimitError

from preprocessing.pdf_elements.chars_maps import superscript_map, subscript_map
from preprocessing.utils.general import get_secret

logger = logging.getLogger()

logger.setLevel(logging.INFO)


class MultiLineTextExtractor:
    class MultiLineTextExtractorError(Exception):
        def __init__(self, message, missing_chars: List[str], response: BaseMessage, used_latex: bool = False):
            self.missing_chars = missing_chars
            self.response = response
            self.used_latex = used_latex
            super().__init__(message)

    def __init__(self, max_retries: int = 2):
        self.multiline_llm_extractor = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=get_secret("OpenAiApiKey", os.getenv('REGION_NAME'))["OPEN_API_KEY"]
        )
        self.system_message = SystemMessage(
            content="You are a helpful assistant that extract text from image"
        )
        self.max_retries = max_retries

    def crop_region_to_base64(self, line_dict: Dict[str, Any], page: pdfplumber.pdf.Page, threshold: float = 2.5,
                              resolution: int = 600) -> str:
        crop_box = (
            line_dict['x0'] - threshold,
            line_dict['top'] - threshold,
            line_dict['x1'] + threshold,
            line_dict['bottom'] + threshold
        )

        cropped_page = page.crop(crop_box)
        image_buffer = io.BytesIO()
        cropped_page.to_image(resolution=resolution).save(image_buffer, format="PNG")
        image_buffer.seek(0)
        return base64.b64encode(image_buffer.read()).decode("utf-8")

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
                            classification.append({"text": subscript_map.get(char['text']), "type": "subscript"})
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

    def parse_llm_response(self, completion: BaseMessage, required_chars: List[str]):

        ## TODO first in document map the chars to this
        char_map = {chr(i): chr(i - 0x1d400 + ord('A')) for i in range(0x1D400, 0x1D419 + 1)}  # Bold A-Z
        char_map.update({chr(i): chr(i - 0x1D41A + ord('a')) for i in range(0x1D41A, 0x1D433 + 1)})  # Bold a-z
        char_map.update({chr(i): chr(i - 0x1D434 + ord('A')) for i in range(0x1D434, 0x1D44D + 1)})  # Italic A-Z
        char_map.update({chr(i): chr(i - 0x1D44E + ord('a')) for i in range(0x1D44E, 0x1D467 + 1)})  # Italic a-z
        char_map.update({
            'Σ': '∑',  # GREEK CAPITAL LETTER SIGMA to N-ARY SUMMATION
            '-': '–',  # HYPHEN-MINUS to EN DASH
            'ɡ': 'g',  # LATIN SMALL LETTER SCRIPT G to LATIN SMALL LETTER G
        })
        missing_chars = []
        char_names_completiotion = []
        mapped_chars = [char_map[char] if char in char_map else char for char in required_chars]
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

        for char in mapped_chars:
            char_name = unicodedata.name(char, "")

            if char not in result:
                missing_chars.append(char)
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

1. **Plain Text Format Only**: Do not use LaTeX or any other formatting under any circumstances. Provide the text in plain text with all visible subscripts and superscripts using Unicode characters.
2. **Subscripts and Superscripts**: 
   - Explicitly retain characters listed as subscripts: {subscript_list}.
   - Explicitly retain characters listed as superscripts: {superscript_list}.
   - Any characters not listed in `subscript_list` or `superscript_list` must appear in their **normal form**.
3. **Correction of Missing or Incorrect Characters**:
   - The following characters are explicitly **not parsed as subscript**: {not_parsed_chars_subscript}. Retain them in their **normal form**.
   - The following characters are explicitly **not parsed as superscript**: {not_parsed_chars_superscript}. Retain them in their **normal form**.
4. **Previous Errors**:
   - In your previous response, you missed certain characters or used incorrect formats. Specifically:
     {f"Missing characters: {[f'{char} :{ord(char)}' for char in missing_chars]}" if missing_chars else ""}
     {f"Incorrect usage of LaTeX or non-plain text formatting. Original response: {prev_completion.content}" if used_latex_before else ""}.
   - Correct these issues in this response.

5. **Retry Guidelines**:
   - Do not use LaTeX or any non-plain text formatting.
   - Ensure all characters, especially `g`, `C`, and any other missing characters, appear in their **normal form** unless explicitly listed as subscript or superscript.
   - If a subscript or superscript does not exist in Unicode, retain the character in its **normal form**.

6. **Output Requirements**:
   - Provide the output as plain text only.
   - Include all visible subscripts and superscripts exactly as they appear.
   - Do not provide any additional explanations, metadata, or comments in your output.

{f'Retrying due to errors in previous response. Missing characters: {missing_chars}, prior response: {prev_completion.content}' if missing_chars else ""}
"""
                },
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_data}"},
                },
            ]
        )

        return self.multiline_llm_extractor.invoke(
            [self.system_message, human_message]
        )

    def parse(self, line_dict: Dict[str, Any], page: pdfplumber.pdf.Page) -> str:
        image_data = self.crop_region_to_base64(line_dict, page)

        superscript_list, subscript_list, regular_char_list, not_parsed_chars_subscript, not_parsed_chars_superscript = \
            self.update_line_dict_with_multilines(line_dict)
        #         RetryOutputParser

        retries = 0
        all_chars = superscript_list + subscript_list + regular_char_list
        completion = self.call_llm(image_data, superscript_list, subscript_list, not_parsed_chars_subscript,
                                   not_parsed_chars_superscript)
        while retries <= self.max_retries:
            try:
                return self.parse_llm_response(completion, all_chars)
            except MultiLineTextExtractor.MultiLineTextExtractorError as e:
                if retries == self.max_retries:
                    raise e
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