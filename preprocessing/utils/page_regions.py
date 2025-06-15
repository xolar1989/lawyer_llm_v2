import io
import logging
import math
import os
import re
from abc import ABC, abstractmethod
from collections import Counter
from enum import Enum
from typing import Union, Tuple, List, Sequence, Dict, Any, Mapping

import cv2
import numpy as np
import pdfplumber
from PIL.Image import Image
from PyPDF2 import PdfFileReader
from numpy import ndarray
from pdfplumber.table import Table

from preprocessing.mongo_db.mongodb import MongodbObject

logger = logging.getLogger()

logger.setLevel(logging.INFO)


## TODO change it to boundaries of the document

class ExceptionalDocument:
    def __init__(self, ELI: str, description: str = "", start_x: float = 0.0, start_y: float = 0.0, end_x: float = 0.0,
                 end_y: float = 0.0):
        self.ELI = ELI
        self.description = description
        self._start_x = start_x
        self._start_y = start_y
        self._end_x = end_x
        self._end_y = end_y

    @property
    def start_x(self) -> float:
        return self._start_x

    @property
    def start_y(self) -> float:
        return self._start_y

    @property
    def end_x(self) -> float:
        return self._end_x

    @property
    def end_y(self) -> float:
        return self._end_y


def get_unusual_documents():
    return [
        ExceptionalDocument(ELI="DU/2016/2073",
                            description="There is no footer date, but header line is in coorect position but without header text",
                            start_x=69.0, end_x=470.0
                            ),
        ExceptionalDocument(ELI="DU/2006/550", description="There is no header line, no footer",
                            start_x=50.0,
                            end_x=480.0
                            )
    ]


def get_unusual_document(document_name) -> ExceptionalDocument:
    unusual_documents = get_unusual_documents()
    try:
        # Use next() to retrieve the matching document
        return next(doc for doc in unusual_documents if doc.ELI == document_name)
    except StopIteration:
        # Raise a ValueError if no document is found
        raise ValueError(f"No unusual document found for '{document_name}'.")


def is_unusual_document(document_name):
    unusual_documents = get_unusual_documents()
    return any(doc.ELI == document_name for doc in unusual_documents)



class TableRegion(MongodbObject):

    def __init__(self, page_regions: List['LegalActPageRegionTableRegion']):
        self._page_regions = page_regions

    @property
    def page_regions(self) -> List['LegalActPageRegionTableRegion']:
        return self._page_regions

    @page_regions.setter
    def page_regions(self, value: List['LegalActPageRegion']):
        self._page_regions = value

    def to_dict(self):
        return {
            "page_regions": [page_region.to_dict() for page_region in self.page_regions]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            page_regions=[LegalActPageRegionTableRegion.from_dict(page_region) for page_region in
                          dict_object['page_regions']]
        )


class ExtractionCase(Enum):
    REGULAR_HEADER = "regular_header"
    HEADER_OVERFLOW = "header_overflow"
    UNUSUAL_DOCUMENT = "unusual_document"
    DOCUMENT_WITH_SPEAKER_RULING = "document_with_speaker_ruling"
    PAGE_WITH_SPEAKER_RULING = "page_with_speaker_ruling"
    ATTACHMENT_WITH_UNUSUAL_HEADER = "attachment_with_unusual_header"
    ATTACHMENT_WITHOUT_HEADER = "attachment_without_header"
    ATTACHMENT_WITH_NORMAL_HEADER = "attachment_with_normal_header"


class LegalActPageRegion(MongodbObject):
    FOOTER_PATTERN = r"\d{2,4}[^\na-zA-Z]*[-\.\/][^\na-zA-Z]*\d{2}[^\na-zA-Z]*[-\.\/][^\na-zA-Z]*\d{2,4}(?=\s*$)"

    FOOTER_PATTERN_NOT_MUST_BE_LAST = r"\d{2,4}[^\na-zA-Z0-9]*[-\.\/][^\na-zA-Z0-9]*\d{2}[^\na-zA-Z0-9]*[-\.\/][^\na-zA-Z0-9]*\d{2,4}"

    NAV_PATTERN_1 = r"[©']\s*K\s*a\s*n\s*c\s*e\s*l\s*a\s*r\s*i\s*a\s+S\s*e\s*j\s*m\s*u\s+s\.\s*\d+\s*/\s*\d+"
    NAV_PATTERN_IN_DOC_WITH_SPEAKER_RULING = r"D\s*z\s*i\s*e\s*n\s*n\s*i\s*k\s+U\s*s\s*t\s*a\s*w(.)*P\s*o\s*z\s*\.\s+\d+"
    NAV_PATTERN_UNUSUAL_ATTACHMENT_FIRST_PAGE = 'L\s*i\s*c\s*z\s*b\s*a\s+s\s*t\s*r\s*o\s*n\s*:\s*\d+\s*D\s*a\s*t\s*a\s*:\s*\d{2,4}[-.]\d{2}[-.]\d{2,4}\s*N\s*a\s*z\s*w\s*a\s+p\s*l\s*i\s*k\s*u\s*:\s*[A-Za-z0-9/.]+\s*.+\s*[IXV]+\s*k\s*a\s*d\s*e\s*n\s*c\s*j\s*a\s*/d\s*r\s*u\s*k'
    NAV_PATTERN_UNUSUAL_ATTACHMENT_REST_PAGES = '\d{1,}\s*\.T\s*O'
    LEGAL_ANNOTATION_PATTERN_1 = r'(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?\)?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)'
    LEGAL_ANNOTATION_PATTERN_2 = r"[\n\r]+(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k)\s*[a-zA-Z\s\d]*[\n\r]+"
    DIFFERENCE_BETWEEN_VERTICAL_LINE_AND_FOOTER = 13
    ACCEPTABLE_DIFFERENCE_BETWEEN_VERTICAL_LINE_AND_HEADER = 5
    MAX_RATIO_FOOTER_REFLECT_HEADER = 0.7

    def __init__(self, start_x: float, start_y: float, end_x: float, end_y: float, page_number: int):
        if start_x >= end_x:
            raise ValueError("start_x must be less than end_x")
        if start_y >= end_y:
            raise ValueError("start_y must be less than end_y")

        self.start_x = start_x
        self.start_y = start_y
        self.end_x = end_x
        self.end_y = end_y
        self.page_number = page_number

    @property
    def bbox(self):
        return self.start_x, self.start_y, self.end_x, self.end_y

    @staticmethod
    def is_correct_position_of_header_towards_right_vertical_line(x_end_header: float,
                                                                  x_right_vertical_line: float) -> bool:
        if x_end_header > x_right_vertical_line \
                + LegalActPageRegion.ACCEPTABLE_DIFFERENCE_BETWEEN_VERTICAL_LINE_AND_HEADER:
            return False
        if x_end_header < x_right_vertical_line - \
                3 * LegalActPageRegion.ACCEPTABLE_DIFFERENCE_BETWEEN_VERTICAL_LINE_AND_HEADER:
            return False

        return True

    @staticmethod
    def locate_footer_date(page: pdfplumber.pdf.Page) -> Union[Tuple[float, float], None]:
        text = page.extract_text()
        words = page.extract_words()
        matches = list(re.finditer(LegalActPageRegion.FOOTER_PATTERN_NOT_MUST_BE_LAST, text))

        if matches:
            footer_text = matches[-1].group(0)
            footer_text = re.sub(r'[\s\n\r]+', '', footer_text)  # Clean spaces and newlines

            # Search for the footer location in the page's layout text
            for idx, word in enumerate(words):
                candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(footer_text.split())])
                if footer_text == candidate_text:
                    footer_x_right = words[idx]['x1']  # right x position of the footer
                    footer_y = words[idx]['top']  # y position of the first line of the footer
                    return footer_x_right, footer_y
            return None
        return None

    @staticmethod
    def get_bottom_y_without_line(page: pdfplumber.pdf.Page) -> float:
        if LegalActPageRegion.locate_footer_date(page):
            footer_x_right, footer_y = LegalActPageRegion.locate_footer_date(page)
            return footer_y
        else:
            return page.height

    @staticmethod
    def get_line_positions(image: ndarray):
        image_gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)

        thresh = cv2.threshold(image_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 1))
        detected_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)

        cnts = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        return sorted(
            [cv2.boundingRect(c) for c in cnts if cv2.boundingRect(c)[2] >= 150],  # Filter based on width
            key=lambda x: x[1]  # Sort by the vertical position (y)
        )

    @staticmethod
    def have_navbar_match(text: str):
        navbar_match = re.search(LegalActPageRegion.NAV_PATTERN_1, text)
        navbar_match_2 = re.search(LegalActPageRegion.NAV_PATTERN_IN_DOC_WITH_SPEAKER_RULING, text)
        return navbar_match or navbar_match_2

    @staticmethod
    def is_document_with_speaker_ruling(text: str):
        return re.search(LegalActPageRegion.NAV_PATTERN_IN_DOC_WITH_SPEAKER_RULING, text)

    @staticmethod
    def is_normal_document(text: str):
        return re.search(LegalActPageRegion.NAV_PATTERN_1, text)

    @staticmethod
    def is_attachment_with_unusual_header(text: str):
        match_first_page = re.search(LegalActPageRegion.NAV_PATTERN_UNUSUAL_ATTACHMENT_FIRST_PAGE, text)
        rest_page_match = re.search(LegalActPageRegion.NAV_PATTERN_UNUSUAL_ATTACHMENT_REST_PAGES, text)
        return match_first_page or rest_page_match

    @staticmethod
    def get_vertical_line_positions(image: ndarray):
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # Step 2: Apply binary thresholding
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

        # Step 3: Morphological operations to group text
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (50, 5))  # Adjust kernel size for horizontal text grouping
        dilated = cv2.dilate(binary, kernel, iterations=1)

        # Step 4: Find contours of text regions
        contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # Step 5: Filter out margin notes
        image_width = image.shape[1]
        margin_threshold = int(image_width * 0.8)  # Filter out regions on the far right (e.g., > 80% of the page width)

        min_x, max_x = image_width, 0

        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if x + w < margin_threshold and w > 30:  # Exclude margin notes and small artifacts
                min_x = min(min_x, x)  # Update min_x
                max_x = max(max_x, x + w)  # Update max_x

        return [min_x, max_x]

    @staticmethod
    def get_extraction_case(
            document_eli: str,
            line_positions_horizontal: List[Sequence[int]],
            page: pdfplumber.pdf.Page,
            scaling_factor: float,
            is_attachment: bool = False
    ) -> ExtractionCase:
        # TODO fix cid, special characters encoding
        text_page = page.extract_text()
        buffer_diff = 2

        if is_attachment:
            if LegalActPageRegion.is_normal_document(text_page) \
                    or LegalActPageRegion.is_document_with_speaker_ruling(text_page):
                return ExtractionCase.ATTACHMENT_WITH_NORMAL_HEADER
            elif LegalActPageRegion.is_attachment_with_unusual_header(text_page):
                return ExtractionCase.ATTACHMENT_WITH_UNUSUAL_HEADER
            else:
                return ExtractionCase.ATTACHMENT_WITHOUT_HEADER
        elif is_unusual_document(document_eli):
            return ExtractionCase.UNUSUAL_DOCUMENT
        elif LegalActPageRegion.is_document_with_speaker_ruling(text_page) and len(line_positions_horizontal) > 0:
            return ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING
        elif LegalActPageRegion.is_normal_document(text_page) and len(line_positions_horizontal) > 0:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
            header_x_end_scaled = (header_x_start + header_width) * scaling_factor
            footer_date_x, footer_date_y = LegalActPageRegion.locate_footer_date(
                page)  ## TODO here in one case is problem
            # args: ((<function apply at 0x7fecc9be50d0>, <bound method EstablishLegalActSegmentsBoundaries.establish_worker of <class '__main__.EstablishLegalActSegmentsBoundaries'>>, [], (<class 'dict'>, [['row', (<class 'dict'>, [['ELI', 'DU/2002/1287'], ['document_year', 2002], ['status', 'akt posiada tekst jednolity'], ['announcementDate', '2002-08-30'], ['volume', 155], ['address', 'WDU20021551287'], ['displayAddress', 'Dz.U. 2002 nr 155 poz. 1287'], ['promulgation', '2002-09-23'], ['pos', 1287], ['publisher', 'DU'], ['changeDate', '2020-12-11T15:29:39'], ['textHTML', True], ['textPDF', True], ['title', 'Ustawa z dnia 30 sierpnia 2002 r. o restrukturyzacji niektórych należności publicznoprawnych od przedsiębiorców.'], ['type', 'Ustawa'], ['filename_of_pdf', 'D20021287Lj.pdf'], ['s3_pdf_path', 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D20021287Lj.pdf']])], ['flow_information', (<class 'dict'>, [['invoke_id', '785dc0ce-d960-44eb-a4b8-475730dc089d'
            # Exception: "TypeError('cannot unpack non-iterable NoneType object')"
            if header_x_end_scaled > footer_date_x + buffer_diff:
                return ExtractionCase.HEADER_OVERFLOW
            elif footer_date_x > header_x_end_scaled + buffer_diff:
                return ExtractionCase.PAGE_WITH_SPEAKER_RULING
            else:
                return ExtractionCase.REGULAR_HEADER
        raise ValueError("Invalid State, extraction case not found")

    @staticmethod
    @abstractmethod
    def build(**kwargs) -> Union['LegalActPageRegion', None]:
        pass

    @staticmethod
    @abstractmethod
    def get_top_y(**kwargs) -> float:
        pass

    @staticmethod
    @abstractmethod
    def get_bottom_y(**kwargs) -> float:
        pass

    @staticmethod
    @abstractmethod
    def get_x_axis(**kwargs) -> Tuple[float, float]:
        pass

    def to_dict(self):
        return {
            "start_x": self.start_x,
            "start_y": self.start_y,
            "end_x": self.end_x,
            "end_y": self.end_y,
            "page_number": self.page_number
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            start_x=dict_object["start_x"],
            start_y=dict_object["start_y"],
            end_x=dict_object["end_x"],
            end_y=dict_object["end_y"],
            page_number=dict_object["page_number"]
        )

    @staticmethod
    def get_scaling_factor(page: pdfplumber.pdf.Page, image: np.ndarray) -> float:
        """
        Calculate the scaling factor to convert pixel coordinates to PDF coordinates
        :param page: pdfplumber.pdf.Page object
        :param image: numpy.ndarray image of the page
        :return: float scaling factor
        """
        return page.height / image.shape[0]


class LegalActPageRegionTableRegion(LegalActPageRegion):

    def __init__(self,
                 start_x: float,
                 start_y: float,
                 end_x: float,
                 end_y: float,
                 page_number: int,
                 start_index: int, end_index: int):
        self.start_index = start_index
        self.end_index = end_index
        super().__init__(
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            page_number=page_number
        )

    @staticmethod
    def build(table: Table, start_index: int, end_index: int) -> Union['LegalActPageRegion', None]:
        pass

    @classmethod
    def have_overlapping_region(cls, evaluated_table: Table,
                                evaluated_table_page_regions: List['LegalActPageRegionTableRegion']) \
            -> bool:
        table_indexes_range = cls.get_indexes_of_table(evaluated_table)
        table_bbox_corrected = (evaluated_table.page.bbox[0],
                                evaluated_table.bbox[1],
                                evaluated_table.page.bbox[2],
                                evaluated_table.bbox[3])
        for i, page_region in enumerate(evaluated_table_page_regions):
            if cls.is_bbox_region_is_overlapping(
                    table_bbox_corrected,
                    page_region.bbox
            ) or cls.is_index_overlapping(
                table_indexes_range,
                (page_region.start_index, page_region.end_index)
            ):
                return True

        return False

    @classmethod
    def update_table_regions(cls, evaluated_table: Table,
                             evaluated_table_page_regions: List['LegalActPageRegionTableRegion']) \
            -> List['LegalActPageRegionTableRegion']:
        ## TODO ended here
        table_indexes_range = cls.get_indexes_of_table(evaluated_table)
        table_bbox_corrected = (evaluated_table.page.bbox[0],
                                evaluated_table.bbox[1],
                                evaluated_table.page.bbox[2],
                                evaluated_table.bbox[3])
        for i, page_region in enumerate(evaluated_table_page_regions):
            if cls.is_bbox_region_is_overlapping(
                    table_bbox_corrected,
                    page_region.bbox
            ) or cls.is_index_overlapping(
                table_indexes_range,
                (page_region.start_index, page_region.end_index)
            ):
                merged_region = cls.update_region(
                    table_indexes_range,
                    table_bbox_corrected,
                    page_region
                )
                evaluated_table_page_regions[i] = merged_region
                return evaluated_table_page_regions
        evaluated_table_page_regions.append(
            LegalActPageRegionTableRegion(
                start_x=table_bbox_corrected[0],
                start_y=table_bbox_corrected[1],
                end_x=table_bbox_corrected[2],
                end_y=table_bbox_corrected[3],
                page_number=evaluated_table.page.page_number,
                start_index=table_indexes_range[0],
                end_index=table_indexes_range[1]
            )
        )
        return evaluated_table_page_regions

    @staticmethod
    def get_indexes_of_table(table: Table):
        page_text = table.page.extract_text()
        table_bbox_corrected = (table.page.bbox[0], table.bbox[1], table.page.bbox[2], table.bbox[3])
        text_of_table = table.page.within_bbox(table_bbox_corrected).extract_text().strip()
        table_start_index = page_text.index(text_of_table)
        table_end_index = table_start_index + len(text_of_table)
        if table_start_index == -1:
            raise ValueError(f"Invalid state: text of table not found in page text, text_of_table: "
                             f"{text_of_table}, page_text: {page_text}, page_number: {table.page.page_number}")
        return table_start_index, table_end_index

    @staticmethod
    def is_bbox_region_is_overlapping(bbox_to_add, bbox_added):
        """Check if two bounding boxes overlap."""
        x1_min, y1_min, x1_max, y1_max = bbox_to_add
        x2_min, y2_min, x2_max, y2_max = bbox_added

        # Check for overlap
        return not (x1_max <= x2_min or x2_max <= x1_min or y1_max <= y2_min or y2_max <= y1_min)

    @staticmethod
    def is_index_overlapping(index_range1, index_range2):
        """Check if two index ranges overlap."""
        start1, end1 = index_range1
        start2, end2 = index_range2

        # Check for overlap
        return not (end1 <= start2 or end2 <= start1)

    @classmethod
    def update_region(cls, table_indexes_range,
                      table_bbox,
                      page_region_to_update: 'LegalActPageRegionTableRegion'):
        table_start_x, table_start_y, table_end_x, table_end_y = table_bbox
        start_index_table, end_index_table = table_indexes_range
        return LegalActPageRegionTableRegion(
            start_x=min(table_start_x, page_region_to_update.start_x),
            start_y=min(table_start_y, page_region_to_update.start_y),
            end_x=max(table_end_x, page_region_to_update.end_x),
            end_y=max(table_end_y, page_region_to_update.end_y),
            page_number=page_region_to_update.page_number,  # Assuming page number is the same
            start_index=min(start_index_table, page_region_to_update.start_index),
            end_index=max(end_index_table, page_region_to_update.end_index)
        )

    def to_dict(self):
        return {
            "start_x": self.start_x,
            "start_y": self.start_y,
            "end_x": self.end_x,
            "end_y": self.end_y,
            "page_number": self.page_number,
            "start_index": self.start_index,
            "end_index": self.end_index
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            start_x=dict_object["start_x"],
            start_y=dict_object["start_y"],
            end_x=dict_object["end_x"],
            end_y=dict_object["end_y"],
            page_number=dict_object["page_number"],
            start_index=dict_object["start_index"],
            end_index=dict_object["end_index"]
        )

    @staticmethod
    def get_top_y(**kwargs) -> float:
        pass

    @staticmethod
    def get_bottom_y(**kwargs) -> float:
        pass

    @staticmethod
    def get_x_axis(**kwargs) -> Tuple[float, float]:
        pass


class LegalActPageRegionParagraphs(LegalActPageRegion):

    @staticmethod
    def build(document_name: str, page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 2) -> Union[
        'LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions_horizontal = LegalActPageRegion.get_line_positions(image_np)
        # line_positions = line_positions_horizontal
        # line_positions_vertical = LegalActPageRegion.get_vertical_line_positions(image_np)
        # text_page = page_pdf.extract_text()
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        ## TODO get position of footer and using it establish right vertical line
        # # Draw lines on the image
        # for (x, y, w, h) in line_positions:
        #     cv2.rectangle(image_np, (x, y), (x + w, y + h), (0, 255, 0), 2)  # Draw a rectangle around each line
        #
        # # Save the modified image to the local directory
        # os.makedirs("./lines", exist_ok=True)
        # output_path = f"./lines/{page_pdf.page_number}.jpg"  # Specify output path
        # cv2.imwrite(output_path, image_np)
        # image = image_np

        # ocr_data = pytesseract.image_to_data(image, output_type=Output.DICT)
        # full_text = " ".join(ocr_data['text'])  # Combine all detected text for regex search
        #
        # # Step 3: Find the footer text using regex
        # footer_match = re.search(LegalActPageRegion.FOOTER_PATTERN, full_text.strip())
        # if footer_match:
        #     footer_text = footer_match.group(0)
        #     footer_text = re.sub(r'[\s\n\r]+', ' ', footer_text)  # Normalize spaces
        #     print("Footer Text Found:", footer_text)
        #
        #     # Step 4: Find the bounding box for the footer text
        #     for i in range(len(ocr_data['text'])):
        #         detected_text = ocr_data['text'][i].strip()
        #         if detected_text in footer_text:  # Match detected word with footer
        #             x, w = ocr_data['left'][i], ocr_data['width'][i]
        #             footer_end_x = x + w  # Calculate x-coordinate of the end of the footer
        #
        #             # Step 5: Draw a vertical line at the x-coordinate
        #             cv2.line(image, (footer_end_x, 0), (footer_end_x, image.shape[0]), (0, 255, 0), 2)
        #             print(f"Footer ends at x: {footer_end_x}")
        #             output_path = f"./lines/{page_pdf.page_number}-22.jpg"
        #             # Step 6: Save the result
        #             cv2.imwrite(output_path, image)
        #             print(f"Image saved to {output_path}")

        # # Step 2: Perform OCR and extract bounding boxes
        # ocr_data = pytesseract.image_to_data(image, output_type=Output.DICT)
        #
        # # Step 3: Initialize variables to find the rightmost x of the main text
        # paragraph_max_x = 0
        # image_width = image.shape[1]
        #
        # for i in range(len(ocr_data['text'])):
        #     if ocr_data['text'][i].strip():  # Only process non-empty text
        #         x, y, w, h = (ocr_data['left'][i], ocr_data['top'][i],
        #                       ocr_data['width'][i], ocr_data['height'][i])
        #         # Check if the text is part of the main text (not the margin)
        #         if x < image_width * 0.8:  # Avoid margin text on the far right
        #             paragraph_max_x = max(paragraph_max_x, x + w)
        #
        # # Step 4: Draw the separator line
        # separator_x = paragraph_max_x + 5  # Add padding to avoid overlap
        # cv2.line(image, (separator_x, 0), (separator_x, image.shape[0]), (0, 255, 0), 2)

        # header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
        # header_x_start = 50.0/scaling_factor
        # header_x_end = 480.0/scaling_factor

        # left_x = header_x_start*scaling_factor
        #
        # right_x = (header_x_start + header_width)*scaling_factor

        # cv2.line(
        #     image,
        #     (int(header_x_start), 0),
        #     (int(header_x_start), image_np.shape[0]),
        #     (0, 255, 0),
        #     2
        # )
        # cv2.line(
        #     image,
        #     (int(header_x_end), 0),
        #     (int(header_x_end), image_np.shape[0]),
        #     (0, 255, 0),
        #     2
        # )
        #
        # output_path = f"./lines/{page_pdf.page_number}-1511.jpg"
        # # Step 7: Save and display the result
        # cv2.imwrite(output_path, image)
        # print(f"Image saved to {output_path}")

        # image = image_np
        #
        # # line_positions_vertical = LegalActPageRegion.get_vertical_line_positions(image)
        # min_x = line_positions_vertical[0]
        # max_x = line_positions_vertical[1]
        # image_width = image.shape[1]
        #
        # print(f"line vertical x_max : {max_x * scaling_factor}")
        #
        # print(f" header max x")
        # if min_x < image_width:  # Ensure min_x was updated
        #     cv2.line(image_np, (min_x, 0), (min_x, image.shape[0]), (255, 0, 0), 2)  # Vertical line at min_x
        # if max_x > 0:  # Ensure max_x was updated
        #     cv2.line(image_np, (max_x, 0), (max_x, image.shape[0]), (255, 0, 0), 2)  # Vertical line at max_x
        #
        # for (x, y, w, h) in line_positions:
        #     cv2.rectangle(image_np, (x, y), (x + w, y + h), (0, 255, 0), 2)  # Draw a rectangle around each line
        #
        # # Step 7: Save and display the result
        # os.makedirs("./lines", exist_ok=True)
        # output_path = f"./lines/{page_pdf.page_number}.jpg"
        # cv2.imwrite(output_path, image_np)

        extraction_case = LegalActPageRegion.get_extraction_case(
            document_eli=document_name,
            line_positions_horizontal=line_positions_horizontal,
            page=page_pdf,
            scaling_factor=scaling_factor
        )
        if extraction_case == ExtractionCase.HEADER_OVERFLOW:
            logger.warning(
                f"{document_name} have header overflow for page: {page_pdf.page_number} in paragraphs region")
        elif extraction_case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            logger.warning(
                f"{document_name} have speaker ruling for page: {page_pdf.page_number} in paragraphs region")
        elif extraction_case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            logger.warning(
                f"{document_name} have page with speaker ruling for page: {page_pdf.page_number} "
                f"in paragraphs region, it usually the first page of the document")
        elif extraction_case == ExtractionCase.UNUSUAL_DOCUMENT:
            logger.warning(
                f"{document_name} have unusual document for page: {page_pdf.page_number} in paragraphs region")

        start_y = LegalActPageRegionParagraphs.get_top_y(
            document_eli=document_name,
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            scaling_factor=scaling_factor
        )

        end_y = LegalActPageRegionParagraphs.get_bottom_y(
            document_name=document_name,
            case=extraction_case,
            page_pdf=page_pdf,
            line_positions_horizontal=line_positions_horizontal,
            scaling_factor=scaling_factor,
            buffer=buffer
        )

        start_x, end_x = LegalActPageRegionParagraphs.get_x_axis(
            document_name=document_name,
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            page_pdf=page_pdf,
            scaling_factor=scaling_factor,
            buffer=buffer
        )

        if extraction_case == ExtractionCase.REGULAR_HEADER or extraction_case == ExtractionCase.HEADER_OVERFLOW:
            page_with_erased_margins = page_pdf.within_bbox((start_x, 0, end_x + buffer, page_pdf.height))
            text_to_check = page_with_erased_margins.extract_text()
            match_last_date_footer = re.search(LegalActPageRegion.FOOTER_PATTERN, text_to_check)
            if not match_last_date_footer:
                # 21:08:09.289 | WARNING | root - Function 'establish_worker' - Attempt 1 failed: Footer date is not last element in document eli: DU/2017/1051, in page: 1. Retrying in 10 seconds...
                raise ValueError(
                    f"Footer date is not last element in document eli: {document_name}, in page: {page_pdf.page_number}")

        return LegalActPageRegionParagraphs(
            start_x=start_x - buffer,
            end_x=end_x + buffer,
            start_y=start_y + buffer,
            end_y=end_y - buffer,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(document_eli: str, case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                  scaling_factor: float) -> float:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.HEADER_OVERFLOW \
                or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
            return header_y * scaling_factor
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            return get_unusual_document(document_eli).start_y

    @staticmethod
    def get_bottom_y(document_name: str, case: ExtractionCase, page_pdf: pdfplumber.pdf.Page,
                     line_positions_horizontal: List[Sequence[int]],
                     scaling_factor: float,
                     buffer: int) -> float:
        """
        Get the bottom Y position of the page
        determined the y of the footer,
        last line_position could be part of table or text !!!
        """

        ## TODO check that footer should be less width than header line, for instance in D19970604Lj.pdf
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.HEADER_OVERFLOW \
                or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING \
                or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            if len(line_positions_horizontal) > 1 and line_positions_horizontal[-1]:
                footer_y_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
                header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
                if not math.isclose(footer_y_start * scaling_factor, header_x_start * scaling_factor,
                                    abs_tol=float(buffer)):
                    return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)
                elif math.isclose(footer_y_start * scaling_factor, header_x_start * scaling_factor,
                                  abs_tol=float(buffer)) and \
                        footer_width * scaling_factor > LegalActPageRegion.MAX_RATIO_FOOTER_REFLECT_HEADER \
                        * header_width * scaling_factor:
                    return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)
                else:
                    return footer_y * scaling_factor
            return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            if len(line_positions_horizontal) > 0:
                footer_y_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
                if footer_y * scaling_factor > 0.3 * page_pdf.height:
                    return footer_y * scaling_factor
                else:
                    return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)
            else:
                return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis(document_name: str, case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                   page_pdf: pdfplumber.pdf.Page, scaling_factor: float, buffer: int) -> Tuple[
        float, float]:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]

            header_x_end = header_x_start + header_width
            return header_x_start * scaling_factor, header_x_end * scaling_factor
        elif case == ExtractionCase.HEADER_OVERFLOW or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
            footer_x_right, footer_y = LegalActPageRegion.locate_footer_date(page_pdf)
            return header_x_start * scaling_factor, footer_x_right + buffer
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            return get_unusual_document(document_name).start_x, get_unusual_document(document_name).end_x


class LegalActPageRegionMarginNotes(LegalActPageRegion):

    @staticmethod
    def build(document_name: str, page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 2) -> Union[
        'LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions_horizontal = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        text_page = page_pdf.extract_text()

        extraction_case = LegalActPageRegion.get_extraction_case(
            document_eli=document_name,
            line_positions_horizontal=line_positions_horizontal,
            page=page_pdf,
            scaling_factor=scaling_factor
        )

        if extraction_case == ExtractionCase.HEADER_OVERFLOW:
            logger.warning(
                f"{document_name} have header overflow for page: {page_pdf.page_number} in margin region")
        elif extraction_case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            logger.warning(
                f"{document_name} have speaker ruling for page: {page_pdf.page_number} in margin region")
        elif extraction_case == ExtractionCase.UNUSUAL_DOCUMENT:
            logger.warning(
                f"{document_name} have unusual document for page: {page_pdf.page_number} in margin region")

        start_y = LegalActPageRegionMarginNotes.get_top_y(
            document_eli=document_name,
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            scaling_factor=scaling_factor
        )

        end_y = LegalActPageRegionMarginNotes.get_bottom_y(
            page=page_pdf,
        )

        start_x, end_x = LegalActPageRegionMarginNotes.get_x_axis(
            document_name=document_name,
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            page_pdf=page_pdf,
            scaling_factor=scaling_factor,
            buffer=buffer
        )

        return LegalActPageRegionMarginNotes(
            start_x=start_x + buffer,
            end_x=end_x,
            start_y=start_y + buffer,
            end_y=end_y,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(document_eli: str, case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                  scaling_factor: float) -> float:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.HEADER_OVERFLOW \
                or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING \
                or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
            return header_y * scaling_factor
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            return get_unusual_document(document_eli).start_y

    @staticmethod
    def get_x_axis(document_name: str, case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                   page_pdf: pdfplumber.pdf.Page, scaling_factor: float, buffer: int) -> Tuple[
        float, float]:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]

            header_x_end = (header_x_start + header_width) * scaling_factor
            if header_x_end >= page_pdf.width:
                raise ValueError("Header position not found, out of page")
            return header_x_end, page_pdf.width
        elif case == ExtractionCase.HEADER_OVERFLOW or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            footer_x_right, footer_y = LegalActPageRegion.locate_footer_date(page_pdf)
            return footer_x_right + buffer, page_pdf.width
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            return get_unusual_document(document_name).end_x, page_pdf.width

    @staticmethod
    def get_bottom_y(page: pdfplumber.pdf.Page) -> float:
        return page.height


class LegalActPageRegionFooterNotes(LegalActPageRegion):

    @staticmethod
    def build(document_name: str, page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 2) -> Union[
        'LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        line_positions_horizontal = LegalActPageRegion.get_line_positions(image_np)

        extraction_case = LegalActPageRegion.get_extraction_case(
            document_eli=document_name,
            line_positions_horizontal=line_positions_horizontal,
            page=page_pdf,
            scaling_factor=scaling_factor
        )

        if extraction_case == ExtractionCase.HEADER_OVERFLOW:
            logger.warning(
                f"{document_name} have header overflow for page: {page_pdf.page_number} in footer region")
        elif extraction_case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            logger.warning(
                f"{document_name} have speaker ruling for page: {page_pdf.page_number} in footer region")
        elif extraction_case == ExtractionCase.UNUSUAL_DOCUMENT:
            logger.warning(
                f"{document_name} have unusual document for page: {page_pdf.page_number} in footer region")

        if extraction_case == ExtractionCase.REGULAR_HEADER or extraction_case == ExtractionCase.HEADER_OVERFLOW \
                or extraction_case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING \
                or extraction_case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            if len(line_positions_horizontal) > 1:
                header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
                footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
                if math.isclose(footer_x_start * scaling_factor, header_x_start * scaling_factor,
                                abs_tol=float(buffer)) and \
                        footer_width * scaling_factor < \
                        LegalActPageRegion.MAX_RATIO_FOOTER_REFLECT_HEADER * header_width * scaling_factor:
                    return LegalActPageRegionFooterNotes.get_footer(
                        document_name=document_name,
                        extraction_case=extraction_case,
                        line_positions_horizontal=line_positions_horizontal,
                        page_pdf=page_pdf,
                        buffer=buffer,
                        scaling_factor=scaling_factor
                    )
                return None
            return None
        elif extraction_case == ExtractionCase.UNUSUAL_DOCUMENT:
            if len(line_positions_horizontal) > 0:
                footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
                if footer_y * scaling_factor > 0.3 * page_pdf.height:
                    return LegalActPageRegionFooterNotes.get_footer(
                        document_name=document_name,
                        extraction_case=extraction_case,
                        line_positions_horizontal=line_positions_horizontal,
                        page_pdf=page_pdf,
                        buffer=buffer,
                        scaling_factor=scaling_factor
                    )
                else:
                    return None
            else:
                return None

        # if extraction_case == ExtractionCase.REGULAR_HEADER or extraction_case == ExtractionCase.HEADER_OVERFLOW:
        #     if len(line_positions_horizontal) > 1:
        #         header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
        #         footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
        #         if math.isclose(footer_x_start * scaling_factor, header_x_start * scaling_factor,
        #                         abs_tol=float(buffer)):
        #             return LegalActPageRegionFooterNotes.get_footer(
        #                 extraction_case=extraction_case,
        #                 line_positions_horizontal=line_positions_horizontal,
        #                 line_positions_vertical=line_positions_vertical,
        #                 page_pdf=page_pdf,
        #                 buffer=buffer,
        #                 scaling_factor=scaling_factor
        #             )
        #         return None
        #     return None
        # elif len(line_positions_horizontal) > 0 and extraction_case == ExtractionCase.NO_HEADER:
        #     footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
        #     if line_positions_vertical[0] * scaling_factor >= footer_x_start * scaling_factor \
        #             - LegalActPageRegion.DIFFERENCE_BETWEEN_VERTICAL_LINE_AND_FOOTER:
        #         return LegalActPageRegionFooterNotes.get_footer(
        #             extraction_case=extraction_case,
        #             line_positions_horizontal=line_positions_horizontal,
        #             line_positions_vertical=line_positions_vertical,
        #             page_pdf=page_pdf,
        #             buffer=buffer,
        #             scaling_factor=scaling_factor
        #         )
        #     return None
        # else:
        #     return None

    @staticmethod
    def get_footer(document_name: str, extraction_case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                   page_pdf: pdfplumber.pdf.Page,
                   buffer: int, scaling_factor: float) -> 'LegalActPageRegionFooterNotes':
        start_x, end_x = LegalActPageRegionFooterNotes.get_x_axis(
            document_name=document_name,
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            page_pdf=page_pdf,
            scaling_factor=scaling_factor,
            buffer=buffer
        )
        start_y = LegalActPageRegionFooterNotes.get_top_y(
            case=extraction_case,
            line_positions_horizontal=line_positions_horizontal,
            buffer=buffer,
            scaling_factor=scaling_factor
        )
        end_y = LegalActPageRegionFooterNotes.get_bottom_y(page_pdf)
        return LegalActPageRegionFooterNotes(
            start_x=start_x - buffer,
            end_x=end_x + buffer,
            start_y=start_y,
            end_y=end_y - buffer,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(case: ExtractionCase, line_positions_horizontal: List[Sequence[int]], buffer: int,
                  scaling_factor: float) -> float:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.HEADER_OVERFLOW \
                or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING \
                or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            if len(line_positions_horizontal) > 1:
                header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
                footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
                if math.isclose(footer_x_start * scaling_factor, header_x_start * scaling_factor,
                                abs_tol=float(buffer)):
                    return footer_y * scaling_factor
                raise ValueError("Invalid State, Header position not found")
            raise ValueError("Invalid State, Header position not found")
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]
            return footer_y * scaling_factor

    @staticmethod
    def get_bottom_y(page_pdf: pdfplumber.pdf.Page) -> float:
        return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis(document_name: str, case: ExtractionCase, line_positions_horizontal: List[Sequence[int]],
                   page_pdf: pdfplumber.pdf.Page, scaling_factor: float, buffer: int) -> Tuple[
        float, float]:
        if case == ExtractionCase.REGULAR_HEADER or case == ExtractionCase.DOCUMENT_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]

            header_x_end = header_x_start + header_width
            return header_x_start * scaling_factor, header_x_end * scaling_factor
        elif case == ExtractionCase.HEADER_OVERFLOW or case == ExtractionCase.PAGE_WITH_SPEAKER_RULING:
            header_x_start, header_y, header_width, _ = line_positions_horizontal[0]
            footer_x_right, footer_y = LegalActPageRegion.locate_footer_date(page_pdf)
            return header_x_start * scaling_factor, footer_x_right + buffer
        elif case == ExtractionCase.UNUSUAL_DOCUMENT:
            return get_unusual_document(document_name).start_x, get_unusual_document(document_name).end_x


class LegalActPageRegionAttachment(LegalActPageRegion):

    @staticmethod
    def build(document_name: str, page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 2) -> Union[
        'LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)

        # # Draw lines on the image
        #         for (x, y, w, h) in line_positions:
        #             cv2.rectangle(image_np, (x, y), (x + w, y + h), (0, 255, 0), 2)  # Draw a rectangle around each line
        #
        #         # Save the modified image to the local directory
        #         os.makedirs("./lines", exist_ok=True)
        #         output_path = f"./lines/{page_pdf.page_number}.jpg"  # Specify output path
        #         cv2.imwrite(output_path, image_np)

        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)

        extraction_case = LegalActPageRegion.get_extraction_case(
            document_eli=document_name,
            line_positions_horizontal=line_positions,
            page=page_pdf,
            scaling_factor=scaling_factor,
            is_attachment=True
        )
        if extraction_case == ExtractionCase.ATTACHMENT_WITHOUT_HEADER:
            logger.warning(
                f"{document_name} have attachment without header for page: {page_pdf.page_number}")
        elif extraction_case == ExtractionCase.ATTACHMENT_WITH_UNUSUAL_HEADER:
            logger.warning(
                f"{document_name} have attachment with unusual header for page: {page_pdf.page_number}")

        top_y = LegalActPageRegionAttachment.get_top_y(extraction_case, line_positions, scaling_factor)
        bottom_y = LegalActPageRegionAttachment.get_bottom_y(extraction_case, page_pdf)
        start_x, end_x = LegalActPageRegionAttachment.get_x_axis(page_pdf)
        return LegalActPageRegionAttachment(
            start_x=start_x,
            end_x=end_x,
            start_y=top_y + buffer,
            end_y=bottom_y - buffer,
            page_number=page_pdf.page_number
        )

    # @classmethod
    # def from_dict(cls, dict_object: Dict[str, Any]):
    #     raise NotImplementedError("Method not implemented")

    @staticmethod
    def get_top_y(extraction_case: ExtractionCase, line_positions: List[Sequence[int]], scaling_factor: float) -> float:
        if extraction_case == ExtractionCase.ATTACHMENT_WITH_NORMAL_HEADER or extraction_case == ExtractionCase.ATTACHMENT_WITH_UNUSUAL_HEADER:
            header_x_start, header_y, header_width, _ = line_positions[0]
            return header_y * scaling_factor
        elif extraction_case == ExtractionCase.ATTACHMENT_WITHOUT_HEADER:
            return 0.0

    @staticmethod
    def get_bottom_y(case: ExtractionCase, page_pdf: pdfplumber.pdf.Page) -> float:
        # TODO some with unusual header have date footer
        if LegalActPageRegion.locate_footer_date(page_pdf):
            footer_x_right, footer_y = LegalActPageRegion.locate_footer_date(page_pdf)
            if footer_y < 0.75 * page_pdf.height:
                return page_pdf.height
            return footer_y
        else:
            return page_pdf.height

    @staticmethod
    def get_x_axis(page_pdf: pdfplumber.pdf.Page) -> Tuple[float, float]:
        return 0.0, page_pdf.width
