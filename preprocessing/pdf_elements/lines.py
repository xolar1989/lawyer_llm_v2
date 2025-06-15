import base64
import io
from abc import ABC, abstractmethod
from typing import List, Mapping, Any, Dict

import pdfplumber
from pdfplumber.table import Table


from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.pdf_elements.chars import CharLegalAct
from PIL import Image

from preprocessing.pdf_elements.chars_maps import superscript_map
from preprocessing.pdf_utils.multiline_extractor import MultiLineTextExtractor


class OrderedObjectLegalAct(MongodbObject, ABC):
    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float,
                 page_number: int, start_index_in_act: int
                 ):
        self.start_x = start_x
        self.end_x = end_x
        self.start_y = start_y
        self.end_y = end_y
        self.page_number = page_number
        self.index_in_act = start_index_in_act

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        if dict_object['line_type'] == 'table':
            return TableRegionPageLegalAct(
                start_x=dict_object['start_x'],
                end_x=dict_object['end_x'],
                start_y=dict_object['start_y'],
                end_y=dict_object['end_y'],
                page_number=dict_object['page_number'],
                start_index_in_act=dict_object['index_in_act'],
                table_number=dict_object['table_number']
            )
        elif dict_object['line_type'] == 'text':
            return TextLinePageLegalAct(
                        start_x=dict_object['start_x'],
                        end_x=dict_object['end_x'],
                        start_y=dict_object['start_y'],
                        end_y=dict_object['end_y'],
                        page_number=dict_object['page_number'],
                        start_index_in_act=dict_object['index_in_act'],
                        chars=[CharLegalAct(**char) for char in dict_object['chars']],
                        merged=dict_object['merged']
            )
        else:
            raise ValueError(f"Invalid line type of object: {dict_object}")

    @property
    @abstractmethod
    def text(self):
        pass

    @property
    def start_index(self):
        return self.index_in_act

    @property
    def end_index(self):
        return self.index_in_act + len(self.text)

    def __len__(self):
        return len(self.text)


class TableRegionPageLegalAct(OrderedObjectLegalAct):

    def to_dict(self):
        return {
            "line_type": "table",
            "start_x": self.start_x,
            "end_x": self.end_x,
            "start_y": self.start_y,
            "end_y": self.end_y,
            "page_number": self.page_number,
            "index_in_act": self.index_in_act,
            "table_number": self.table_number
        }

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float,
                 page_number: int, start_index_in_act: int,
                 table_number: int):
        super().__init__(start_x, end_x, start_y, end_y, page_number, start_index_in_act)
        self.table_number = table_number

    @property
    def text(self):
        return f" | Table {self.table_number} | "

    @property
    def bbox(self):
        return self.start_x, self.start_y, self.end_x, self.end_y


class TextLinePageLegalAct(OrderedObjectLegalAct):

    def to_dict(self):
        return {
            "line_type": "text",
            "start_x": self.start_x,
            "end_x": self.end_x,
            "start_y": self.start_y,
            "end_y": self.end_y,
            "page_number": self.page_number,
            "index_in_act": self.index_in_act,
            "chars": [char.__dict__ for char in self.chars],
            "merged": self.merged
        }

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float,
                 page_number: int, start_index_in_act: int, chars: List[CharLegalAct],
                 merged: bool = False
                 ):
        super().__init__(start_x, end_x, start_y, end_y, page_number, start_index_in_act)
        self.chars = chars
        self.merged = merged

    @property
    def text(self):
        return "".join([char.text for char in self.chars])

    @classmethod
    def find_next_non_space_char(cls, char_index, index_of_char, text_line: Dict[str, Any]) -> float:

        text_of_line = text_line['text'].strip()
        for next_char_index in range(char_index + 1, len(text_of_line)):
            if text_of_line[next_char_index] != '':
                r = text_line['chars'][index_of_char]
                return text_line['chars'][index_of_char]['x0']

    @staticmethod
    def image_to_base64(image_path):
        """
        Reads an image from the specified path, loads it into a buffer,
        and encodes it to a Base64 string.

        :param image_path: Path to the image file
        :return: Base64 encoded string of the image
        """
        # Open the image using PIL
        with Image.open(image_path) as img:
            # Create a buffer to hold the image data
            image_buffer = io.BytesIO()
            # Save the image into the buffer in PNG format
            img.save(image_buffer, format="PNG")
            # Move the buffer's position to the start
            image_buffer.seek(0)
            # Encode the image data from the buffer to Base64
            base64_encoded_image = base64.b64encode(image_buffer.read()).decode("utf-8")
            return base64_encoded_image

    @classmethod
    def crop_region_to_base64(cls, line_dict: Dict[str, Any], page: pdfplumber.pdf.Page, threshold: float = 2.5,
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

    @classmethod
    def build_using_single_line(cls, line_dict: Dict[str, Any], page: pdfplumber.pdf.Page,
                                current_index: int,
                                threshold: float = 2.5) -> 'TextLinePageLegalAct':

        text_of_line = line_dict['text'].strip()
        index_of_char = 0
        char_obj_list = []
        x1_last: float = -1.0

        for char_index, char_text in enumerate(text_of_line):
            if char_text != ' ':
                char_in_chars = line_dict['chars'][index_of_char]

                text_for_obj_char = None

                ## TODO think about  superscript of comma
                if char_in_chars['bottom'] + threshold < line_dict['bottom']:
                    text_for_obj_char = superscript_map.get(char_text)
                else:
                    text_for_obj_char = char_text
                char_obj = CharLegalAct(
                    x0=char_in_chars['x0'],
                    x1=char_in_chars['x1'],
                    bottom=char_in_chars['bottom'],
                    top=char_in_chars['top'],
                    text=text_for_obj_char,
                    index_in_legal_act=current_index + char_index
                )
                char_obj_list.append(char_obj)
                x1_last = char_in_chars['x1']
                index_of_char += 1
            else:

                # for next_char_index in range(char_index+1, len(text_of_line['chars'])):
                #     if char_text != '':
                #         return text_of_line['chars'][index_of_char]['xO']

                s = cls.find_next_non_space_char(char_index, index_of_char, line_dict)
                char_obj = CharLegalAct(
                    x0=x1_last,
                    x1=cls.find_next_non_space_char(char_index, index_of_char, line_dict),
                    bottom=line_dict['bottom'],
                    top=line_dict['top'],
                    text=char_text,
                    index_in_legal_act=current_index + char_index
                )
                char_obj_list.append(char_obj)

        return cls(
            start_x=line_dict['x0'],
            end_x=line_dict['x1'],
            start_y=line_dict['top'],
            end_y=line_dict['bottom'],
            chars=char_obj_list,
            page_number=page.page_number,
            start_index_in_act=current_index
        )

    @classmethod
    def build_using_multi_line(cls, line_dict: Dict[str, Any], page: pdfplumber.pdf.Page,
                               current_index: int,
                               multiline_llm_extractor: MultiLineTextExtractor) -> 'TextLinePageLegalAct':
        # TODO InMemoryRateLimiter ## add this take into account dask_cluster workers amount = for example 12 , max tokens limit per minute 450000 and let's say one invocation is around 1000 tokens in this case:
        #    for one worker for one second we have 625 tokens, so for each worker we should accept one invokaction at each 1.5-2 seconds
        multiline_from_llm = multiline_llm_extractor.parse(line_dict, page)
        line_x0 = line_dict['x0']
        line_x1 = line_dict['x1']
        char_multiline_width = (line_x1 - line_x0) / len(multiline_from_llm)
        char_obj_list = []
        for char_index, char in enumerate(multiline_from_llm):
            char_obj = CharLegalAct(
                x0=line_x0 + char_index * char_multiline_width,
                x1=line_x0 + (char_index + 1) * char_multiline_width,
                bottom=line_dict['bottom'],
                top=line_dict['top'],
                text=char,
                index_in_legal_act=current_index + char_index
            )
            char_obj_list.append(char_obj)
        return cls(
            start_x=line_dict['x0'],
            end_x=line_dict['x1'],
            start_y=line_dict['top'],
            end_y=line_dict['bottom'],
            chars=char_obj_list,
            page_number=page.page_number,
            start_index_in_act=current_index,
            merged=True
        )