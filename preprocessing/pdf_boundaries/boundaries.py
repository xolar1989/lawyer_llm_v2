from abc import ABC

import pdfplumber


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