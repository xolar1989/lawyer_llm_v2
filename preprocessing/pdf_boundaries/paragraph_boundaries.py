from enum import Enum
from typing import List, Union, Tuple

import pdfplumber
from pdfplumber.table import Table

from preprocessing.pdf_boundaries.boundaries import BoundariesArea
from preprocessing.pdf_utils.table_utils import TableDetector, TableCoordinates
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs


class ParagraphAreaType(Enum):
    REGULAR = 1
    TABLE = 2


class LegalActParagraphArea(BoundariesArea):

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float, page_number: int,
                 area_type: ParagraphAreaType):
        super().__init__(start_x, end_x, start_y, end_y, page_number)
        self.area_type = area_type

    @classmethod
    def split_into_areas(cls, paragraph: LegalActPageRegionParagraphs, page: pdfplumber.pdf.Page,
                         tables_on_page: List[TableCoordinates]) -> List['LegalActParagraphArea']:
        current_start_y = paragraph.start_y
        current_end_y = paragraph.end_y

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
