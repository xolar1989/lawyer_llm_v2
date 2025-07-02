from typing import List, Any

import pdfplumber

from preprocessing.pdf_boundaries.boundaries import BoundariesArea
from preprocessing.utils.page_regions import LegalActPageRegionFooterNotes


class LegalActFooterArea(BoundariesArea):

    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float, page_number: int):
        super().__init__(start_x, end_x, start_y, end_y, page_number)

    @classmethod
    def split_into_areas(cls, footer_notes: LegalActPageRegionFooterNotes) -> List['LegalActFooterArea']:
        areas = []
        if footer_notes is not None:
            areas.append(cls(
                start_x=footer_notes.start_x,
                start_y=footer_notes.start_y,
                end_x=footer_notes.end_x,
                end_y=footer_notes.end_y,
                page_number=footer_notes.page_number
            ))
        return areas

