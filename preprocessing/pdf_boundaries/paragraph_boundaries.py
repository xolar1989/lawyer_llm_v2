from enum import Enum
from typing import List, Union, Tuple

import pdfplumber
from pdfplumber.table import Table

from preprocessing.pdf_utils.table_utils import TableDetector
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs


class ParagraphAreaType(Enum):
    REGULAR = 1
    TABLE = 2


class LegalActParagraphArea:

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float,
                 page_number: int, area_type: ParagraphAreaType):
        self.start_x = start_x
        self.end_x = end_x
        self.start_y = start_y
        self.end_y = end_y
        self.page_number = page_number
        self.area_type = area_type

    @property
    def bbox(self):
        return self.start_x, self.start_y, self.end_x, self.end_y

    @classmethod
    def _is_there_area(cls, page: pdfplumber.pdf.Page, area_bbox: tuple) -> bool:
        area = page.within_bbox(area_bbox)
        area_text = area.extract_text().strip()
        return bool(area_text)

    @classmethod
    def split_paragraph_into_areas(cls, paragraph: LegalActPageRegionParagraphs, page: pdfplumber.pdf.Page) -> List['LegalActParagraphArea']:
        current_start_y = paragraph.start_y
        current_end_y = paragraph.end_y

        tables_on_page = TableDetector.extract(page, paragraph)

        areas: List['LegalActParagraphArea'] = []
        for table in tables_on_page:

            table_start_y = table.bbox[1]
            table_end_y = table.bbox[3]
            prev_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, table_start_y)
            if current_start_y <= table_start_y and cls._is_there_area(page, prev_bbox):
                areas.append(LegalActParagraphArea(
                    start_x=paragraph.start_x,
                    end_x=paragraph.end_x,
                    start_y=paragraph.start_y,
                    end_y=table_start_y,
                    page_number=paragraph.page_number,
                    area_type=ParagraphAreaType.REGULAR)
                )
            if cls._is_there_area(page, table.bbox):
                areas.append(LegalActParagraphArea(
                    start_x=table.bbox[0],
                    end_x=table.bbox[2],
                    start_y=table.bbox[1],
                    end_y=table.bbox[3],
                    page_number=paragraph.page_number,
                    area_type=ParagraphAreaType.TABLE)
                )
            else:
                raise ValueError(
                    f"Invalid state: table not found in page text, table bbox: {table.bbox}, page_number: {paragraph.page_number}")
            current_start_y = table_end_y

        last_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, paragraph.end_y)
        if current_start_y < current_end_y and cls._is_there_area(page, last_bbox):
            areas.append(LegalActParagraphArea(
                start_x=paragraph.start_x,
                end_x=paragraph.end_x,
                start_y=current_start_y,
                end_y=paragraph.end_y,
                page_number=paragraph.page_number,
                area_type=ParagraphAreaType.REGULAR)
            )
        return areas
