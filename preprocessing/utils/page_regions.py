import math
import re
from abc import ABC, abstractmethod
from typing import Union, Tuple, List, Sequence

import cv2
import numpy as np
import pdfplumber
from PIL.Image import Image
from numpy import ndarray


class LegalActPageRegion(ABC):
    FOOTER_PATTERN = r"\d{2,4}\s*-\s*\d{2}\s*-\s*\d{2}$"
    NAV_PATTERN = r"©\s*K\s*a\s*n\s*c\s*e\s*l\s*a\s*r\s*i\s*a\s+S\s*e\s*j\s*m\s*u\s+s\.\s*\d+\s*/\s*\d+"
    LEGAL_ANNOTATION_PATTERN = r"(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[" \
                               r"a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[" \
                               r"aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)"


    def __init__(self, start_x: float, start_y: float, end_x: float, end_y: float, page_number: int):
        self.start_x = start_x
        self.start_y = start_y
        self.end_x = end_x
        self.end_y = end_y
        self.page_number = page_number

    @staticmethod
    def get_bottom_y_without_line(page: pdfplumber.pdf.Page) -> float:
        footer_y = page.bbox[3]
        text = page.extract_text()
        words = page.extract_words()
        footer_match = re.search(LegalActPageRegion.FOOTER_PATTERN, text)
        if footer_match:
            footer_text = footer_match.group(0)
            footer_text = re.sub(r'[\s\n\r]+', ' ', footer_text)  # Clean spaces and newlines

            # Search for the footer location in the page's layout text
            for idx, word in enumerate(words):
                candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(footer_text.split())])
                if footer_text == candidate_text:
                    footer_y = words[idx]['top']  # Y position of the first line of the footer
                    break

            return footer_y
        else:
            return page.bbox[3]

    @staticmethod
    def get_line_positions(image: ndarray):
        image_gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)

        thresh = cv2.threshold(image_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 1))
        detected_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)

        cnts = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        return sorted([cv2.boundingRect(c) for c in cnts], key=lambda x: x[1])

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

    @staticmethod
    def get_scaling_factor(page: pdfplumber.pdf.Page, image: np.ndarray) -> float:
        """
        Calculate the scaling factor to convert pixel coordinates to PDF coordinates
        :param page: pdfplumber.pdf.Page object
        :param image: numpy.ndarray image of the page
        :return: float scaling factor
        """
        return page.height / image.shape[0]


class LegalActPageRegionParagraphs(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)

        header_y_pdf = LegalActPageRegionParagraphs.get_top_y(line_positions, scaling_factor)
        footer_y_pdf = LegalActPageRegionParagraphs.get_bottom_y(page_pdf,
                                                                 line_positions,
                                                                 scaling_factor, buffer)
        header_x_start_pdf, header_x_end_pdf = LegalActPageRegionParagraphs.get_x_axis(line_positions[0],
                                                                                       scaling_factor)
        return LegalActPageRegionParagraphs(
            start_x=header_x_start_pdf - buffer,
            end_x=header_x_end_pdf + buffer,
            start_y=header_y_pdf + buffer,
            end_y=footer_y_pdf - buffer,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(line_positions: List[Sequence[int]], scaling_factor: float) -> float:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]
            return header_y * scaling_factor
        else:
            raise ValueError("Header position not found")

    @staticmethod
    def get_bottom_y(page: pdfplumber.pdf.Page, line_positions: List[Sequence[int]], scaling_factor: float,
                     buffer: int) -> float:
        """
        Get the bottom Y position of the page
        determined the y of the footer,
        last line_position could be part of table or text !!!
        :param page:
        :param line_positions:
        :param scaling_factor:
        :param buffer:
        :return:
        """
        if len(line_positions) > 1 and line_positions[-1]:
            footer_y_start, footer_y, footer_width, _ = line_positions[-1]
            header_x_start, header_y, header_width, _ = line_positions[0]
            if not math.isclose(footer_y_start * scaling_factor, header_x_start * scaling_factor,
                                abs_tol=float(buffer)):
                return LegalActPageRegionParagraphs.get_bottom_y_without_line(page)
            else:
                return footer_y * scaling_factor
        return LegalActPageRegionParagraphs.get_bottom_y_without_line(page)

    @staticmethod
    def get_x_axis(line_header_position: Sequence[int], scaling_factor: float) -> Tuple[float, float]:
        if line_header_position:
            header_x_start, header_y, header_width, _ = line_header_position

            header_x_end = header_x_start + header_width
            return header_x_start * scaling_factor, header_x_end * scaling_factor
        else:
            raise ValueError("Header position not found")


class LegalActPageRegionMarginNotes(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        y_start = LegalActPageRegionMarginNotes.get_top_y(line_positions, scaling_factor)
        x_start, x_end = LegalActPageRegionMarginNotes.get_x_axis(page_pdf, line_positions, scaling_factor)
        y_end = LegalActPageRegionMarginNotes.get_bottom_y(page_pdf)
        return LegalActPageRegionMarginNotes(
            start_x=x_start + buffer,
            end_x=x_end,
            start_y=y_start + buffer,
            end_y=y_end,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(line_positions: List[Sequence[int]], scaling_factor: float) -> float:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]
            return header_y * scaling_factor
        else:
            raise ValueError("Header position not found")

    @staticmethod
    def get_x_axis(page_pdf: pdfplumber.pdf.Page, line_positions: List[Sequence[int]], scaling_factor: float) -> Tuple[
        float, float]:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]

            header_x_end = header_x_start + header_width
            if header_x_end * scaling_factor >= page_pdf.width:
                raise ValueError("Header position not found, out of page")
            return header_x_end * scaling_factor, page_pdf.width
        else:
            raise ValueError("Header position not found")

    @staticmethod
    def get_bottom_y(page: pdfplumber.pdf.Page) -> float:
        return page.height


class LegalActPageRegionFooterNotes(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        if len(line_positions) > 1 and line_positions[-1]:
            footer_x_start, footer_y, footer_width, _ = line_positions[-1]
            header_x_start, header_y, header_width, _ = line_positions[0]
            if math.isclose(footer_x_start * scaling_factor, header_x_start * scaling_factor,
                            abs_tol=float(buffer)):
                top_y = LegalActPageRegionFooterNotes.get_top_y(footer_y, scaling_factor)
                x_start, x_end = LegalActPageRegionFooterNotes.get_x_axis(line_positions, scaling_factor)
                y_end = LegalActPageRegionFooterNotes.get_bottom_y(page_pdf)
                return LegalActPageRegionFooterNotes(
                    start_x=x_start - buffer,
                    end_x=x_end + buffer,
                    start_y=top_y,
                    end_y=y_end - buffer,
                    page_number=page_pdf.page_number
                )
        return None

    @staticmethod
    def get_top_y(footer_y_start: float, scaling_factor: float) -> float:
        return footer_y_start * scaling_factor

    @staticmethod
    def get_bottom_y(page_pdf: pdfplumber.pdf.Page) -> float:
        return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis(line_positions: List[Sequence[int]], scaling_factor: float) -> Tuple[float, float]:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]

            header_x_end = header_x_start + header_width
            return header_x_start * scaling_factor, header_x_end * scaling_factor
        else:
            raise ValueError("Header position not found")


class LegalActPageRegionAttachment(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = LegalActPageRegion.get_scaling_factor(page_pdf, image_np)
        top_y = LegalActPageRegionAttachment.get_top_y(page_pdf, line_positions, scaling_factor)
        bottom_y = LegalActPageRegionAttachment.get_bottom_y(page_pdf)
        start_x, end_x = LegalActPageRegionAttachment.get_x_axis(page_pdf)
        return LegalActPageRegionAttachment(
            start_x=start_x,
            end_x=end_x,
            start_y=top_y + buffer,
            end_y=bottom_y - buffer,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(page_pdf: pdfplumber.pdf.Page, line_positions: List[Sequence[int]], scaling_factor: float) -> float:
        text = page_pdf.extract_text()

        # Find Navbar (Header) Position
        navbar_match = re.search(LegalActPageRegion.NAV_PATTERN, text)
        if navbar_match:
            if len(line_positions) > 0 and line_positions[0]:
                header_x_start, header_y, header_width, _ = line_positions[0]
                return header_y * scaling_factor
            else:
                raise ValueError("Header position not found")
        else:
            return 0.0

    @staticmethod
    def get_bottom_y(page_pdf: pdfplumber.pdf.Page) -> float:
        return LegalActPageRegion.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis(page_pdf: pdfplumber.pdf.Page) -> Tuple[float, float]:
        return 0.0, page_pdf.width
