import string
from abc import ABC, abstractmethod
from collections import Counter
from typing import Any, List

import pdfplumber

from preprocessing.pdf_elements.chars import CharLegalAct


class BoundariesArea(ABC):

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float,
                 page_number: int):
        self.start_x = start_x
        self.end_x = end_x
        self.start_y = start_y
        self.end_y = end_y
        self.page_number = page_number

    @property
    def bbox(self):
        return self.start_x, self.start_y, self.end_x, self.end_y

    @classmethod
    def _is_there_area(cls, page: pdfplumber.pdf.Page, area_bbox: tuple) -> bool:
        area = page.within_bbox(area_bbox)
        area_text = area.extract_text().strip()
        return bool(area_text)

    def get_heights_of_chars(self, page: pdfplumber.pdf.Page) -> Counter:
        text_lines = page.within_bbox(self.bbox).extract_text_lines()
        text_lines = [
            {
                **line,
                'merged': False,
                'chars': [{**char, 'num_line': 1} for char in line['chars']]
            }
            for line in sorted(text_lines, key=lambda x: x['top'])
        ]
        heights = [
            CharLegalAct.get_height_from_dict(char)
            for line in text_lines
            for char in line["chars"]
            if char.get("text") not in string.whitespace
        ]
        height_counts = Counter(heights)
        return height_counts

    @classmethod
    @abstractmethod
    def split_into_areas(cls, **kwargs) -> List[Any]:
        pass
