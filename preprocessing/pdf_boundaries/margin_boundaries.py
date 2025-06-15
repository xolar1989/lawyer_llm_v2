from typing import List

import pdfplumber

from preprocessing.pdf_boundaries.boundaries import BoundariesArea
from preprocessing.pdf_utils.table_utils import TableDetector
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs, LegalActPageRegionMarginNotes


class LegalActMarginArea(BoundariesArea):

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float, page_number: int):
        super().__init__(start_x, end_x, start_y, end_y, page_number)

    @classmethod
    def split_margin_notes_into_areas(cls, paragraph: LegalActPageRegionParagraphs,
                                      margin: LegalActPageRegionMarginNotes,
                                      page: pdfplumber.pdf.Page) -> List['LegalActMarginArea']:
        current_start_y = margin.start_y
        current_end_y = margin.end_y

        tables_on_page = TableDetector.extract(page, paragraph)

        areas: List['LegalActMarginArea'] = []
        for table_index, table in enumerate(tables_on_page):

            table_start_y = table.bbox[1]
            table_end_y = table.bbox[3]
            if current_start_y <= table_start_y:
                if table.end_x > margin.start_x:
                    areas.append(LegalActMarginArea(
                        start_x=margin.start_x,
                        end_x=margin.end_x,
                        start_y=margin.start_y,
                        end_y=table_start_y,
                        page_number=paragraph.page_number)
                    )
                    areas.append(LegalActMarginArea(
                        start_x=table.end_x,
                        end_x=margin.end_x,
                        start_y=table.start_y,
                        end_y=table.end_y,
                        page_number=paragraph.page_number)
                    )
                    current_start_y = table_end_y
                    if table_index + 1 < len(tables_on_page):
                        next_table = tables_on_page[table_index + 1]
                        current_end_y = next_table.start_y
                    else:
                        current_end_y = margin.end_y
                else:
                    if table_index + 1 < len(tables_on_page):
                        next_table = tables_on_page[table_index + 1]
                        current_end_y = next_table.start_y
                    else:
                        current_end_y = margin.end_y

        if current_start_y < current_end_y:
            areas.append(LegalActMarginArea(
                start_x=margin.start_x,
                end_x=margin.end_x,
                start_y=current_start_y,
                end_y=current_end_y,
                page_number=paragraph.page_number)
            )
        return areas
