import io
import math
import time
from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from io import BytesIO
from typing import Sequence, Set

import boto3
import numpy as np
import pandas as pd
import dask.dataframe as dd
from PIL.Image import Image
from dask.distributed import Client
from numpy import ndarray

from spire.pdf import *

from preprocessing.kkk import get_storage_options_for_ddf_dask
from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.utils.datalake import Datalake
from preprocessing.utils.defaults import DATALAKE_BUCKET, SEJM_API_URL, AWS_REGION
from preprocessing.mongo_db.mongodb import MongodbCollection, MongodbCollectionIndex
from preprocessing.mongo_db.mongodb_schema import extract_text_and_table_page_number_stage_schema

from preprocessing.utils.s3_helper import upload_json_to_s3, extract_bucket_and_key
from pymongo import ASCENDING

from preprocessing.utils.sejm_api_utils import make_api_call
# from bs4 import BeautifulSoup
from pdf2image import convert_from_bytes
import cv2

import pdfplumber

import re
from PyPDF2 import PdfFileReader, PdfFileWriter

# Define the regex pattern for identifying "Załącznik" sections
LEGAL_ANNOTATION_PATTERN = r"(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)"

NAV_PATTERN = r"©\s*K\s*a\s*n\s*c\s*e\s*l\s*a\s*r\s*i\s*a\s+S\s*e\s*j\s*m\s*u\s+s\.\s*\d+\s*/\s*\d+"

FOOTER_PATTERN = r"\d{2,4}\s*-\s*\d{2}\s*-\s*\d{2}$"

header_x_start_tab = {}
footer_x_start_tab = {}
footer_width_tab = {}


class LegalActPageRegion(ABC):
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
        footer_match = re.search(FOOTER_PATTERN, text)
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
        return sorted(
            [cv2.boundingRect(c) for c in cnts if cv2.boundingRect(c)[2] >= 150],  # Filter based on width
            key=lambda x: x[1]  # Sort by the vertical position (y)
        )

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
    @abstractmethod
    def get_top_y_using_vertical_lines(**kwargs) -> float:
        pass

    @staticmethod
    @abstractmethod
    def get_bottom_y_using_vertical_lines(**kwargs) -> float:
        pass

    @staticmethod
    @abstractmethod
    def get_x_axis_using_vertical_lines(**kwargs) -> Tuple[float, float]:
        pass


class LegalActPageRegionParagraphs(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = get_scaling_factor(page_pdf, image_np)

        # Draw lines on the image
        for (x, y, w, h) in line_positions:
            cv2.rectangle(image_np, (x, y), (x + w, y + h), (0, 255, 0), 2)  # Draw a rectangle around each line

            # Save the modified image to the local directory
        os.makedirs("./lines", exist_ok=True)
        output_path = f"./lines/{page_pdf.page_number}.jpg"  # Specify output path
        cv2.imwrite(output_path, image_np)

        image = image_np

        line_positions_vertical = LegalActPageRegion.get_vertical_line_positions(image)
        min_x = line_positions_vertical[0]
        max_x = line_positions_vertical[1]
        image_width = image.shape[1]

        ## TODO leave it here !!!!!!!
        # Step 6: Draw vertical lines
        output_image = image.copy()
        if min_x < image_width:  # Ensure min_x was updated
            cv2.line(output_image, (min_x, 0), (min_x, image.shape[0]), (255, 0, 0), 2)  # Vertical line at min_x
        if max_x > 0:  # Ensure max_x was updated
            cv2.line(output_image, (max_x, 0), (max_x, image.shape[0]), (255, 0, 0), 2)  # Vertical line at max_x

        # Step 7: Save and display the result
        output_path = f"./lines_2/{page_pdf.page_number}.jpg"
        cv2.imwrite(output_path, output_image)

        # LegalActPageRegionParagraphs.get_bottom_y_using_vertical_lines(line_positions, line_positions_vertical, scaling_factor)

        try:
            top_y = LegalActPageRegionParagraphs.get_top_y(line_positions, page_pdf, scaling_factor)
            bottom_y = LegalActPageRegionParagraphs.get_bottom_y(page_pdf,
                                                                 line_positions,
                                                                 scaling_factor, buffer)
            start_x, end_x = LegalActPageRegionParagraphs.get_x_axis(line_positions[0], scaling_factor)
        except Exception as e:
            print(f"Error in page {page_pdf.page_number}: {e}")
            print(f"Using Vertical lines for {page_pdf.page_number}")
            top_y = LegalActPageRegionParagraphs.get_top_y_using_vertical_lines(line_positions_vertical, scaling_factor)
            bottom_y = LegalActPageRegionParagraphs.get_bottom_y_using_vertical_lines(line_positions,
                                                                                      line_positions_vertical, page_pdf,
                                                                                      scaling_factor)
            start_x, end_x = LegalActPageRegionParagraphs.get_x_axis_using_vertical_lines(
                line_positions_vertical,
                scaling_factor)

        return LegalActPageRegionParagraphs(
            start_x=start_x - buffer,
            end_x=end_x + buffer,
            start_y=top_y + buffer,
            end_y=bottom_y - buffer,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y_using_vertical_lines(line_positions_vertical: List[int], scaling_factor: float) -> float:
        return 0.0

    @staticmethod
    def get_bottom_y_using_vertical_lines(line_positions_horizontal: List[Sequence[int]],
                                          line_positions_vertical: List[int], page_pdf: pdfplumber.pdf.Page,
                                          scaling_factor: float) -> float:
        if len(line_positions_vertical) != 2 or line_positions_vertical[1] <= line_positions_vertical[0]:
            raise ValueError("Line positions vertical not found, or have invalid values")
        if len(line_positions_horizontal) > 0 and line_positions_horizontal[-1]:
            footer_x_start, footer_y, footer_width, _ = line_positions_horizontal[-1]

            footer_x_start = footer_x_start * scaling_factor
            left_line_x = line_positions_vertical[0]
            left_line_x = left_line_x * scaling_factor
            if left_line_x < footer_x_start - 13:
                return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)
            return footer_y * scaling_factor
        return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis_using_vertical_lines(line_positions_vertical: List[int], scaling_factor: float) -> Tuple[
        float, float]:
        if len(line_positions_vertical) != 2 or line_positions_vertical[1] <= line_positions_vertical[0]:
            raise ValueError("Line positions vertical not found, or have invalid values")
        return line_positions_vertical[0] * scaling_factor, line_positions_vertical[1] * scaling_factor

    @staticmethod
    def get_top_y(line_positions: List[Sequence[int]], page_pdf: pdfplumber.pdf.Page, scaling_factor: float) -> float:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]
            if header_y * scaling_factor > page_pdf.height * 0.2:
                raise ValueError("Header position not found, line is not detected in correct position")

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
            footer_x_start_tab[page.page_number] = footer_y_start * scaling_factor
            footer_width_tab[page.page_number] = footer_width * scaling_factor
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
        scaling_factor = get_scaling_factor(page_pdf, image_np)
        try:
            y_start = LegalActPageRegionMarginNotes.get_top_y(line_positions, scaling_factor)
            x_start, x_end = LegalActPageRegionMarginNotes.get_x_axis(page_pdf, line_positions, scaling_factor)
            y_end = LegalActPageRegionMarginNotes.get_bottom_y(page_pdf)
        except Exception as e:
            print(f"Error in page {page_pdf.page_number}: {e}")
            print(f"Using Vertical lines for {page_pdf.page_number}")
            vertical_line_positions = LegalActPageRegion.get_vertical_line_positions(image_np)
            y_start = LegalActPageRegionMarginNotes.get_top_y_using_vertical_lines()
            x_start, x_end = LegalActPageRegionMarginNotes \
                .get_x_axis_using_vertical_lines(page_pdf,
                                                 vertical_line_positions,
                                                 scaling_factor)
            y_end = LegalActPageRegionMarginNotes.get_bottom_y_using_vertical_lines(page_pdf)
        return LegalActPageRegionMarginNotes(
            start_x=x_start + buffer,
            end_x=x_end,
            start_y=y_start + buffer,
            end_y=y_end,
            page_number=page_pdf.page_number
        )

    @staticmethod
    def get_top_y(line_positions: List[Sequence[int]], page_pdf: pdfplumber.pdf.Page, scaling_factor: float) -> float:
        if len(line_positions) > 0 and line_positions[0]:
            header_x_start, header_y, header_width, _ = line_positions[0]
            if header_y * scaling_factor > page_pdf.height * 0.2:
                raise ValueError("Header position not found, line is not detected in correct position")
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

    @staticmethod
    def get_top_y_using_vertical_lines() -> float:
        return 0.0

    @staticmethod
    def get_bottom_y_using_vertical_lines(page: pdfplumber.pdf.Page) -> float:
        return page.height

    @staticmethod
    def get_x_axis_using_vertical_lines(page_pdf: pdfplumber.pdf.Page, line_positions_vertical: List[int],
                                        scaling_factor: float) -> Tuple[
        float, float]:
        if len(line_positions_vertical) != 2 or line_positions_vertical[1] <= line_positions_vertical[0]:
            raise ValueError("Line positions vertical not found, or have invalid values")
        if line_positions_vertical[1] * scaling_factor >= page_pdf.width:
            raise ValueError("Header position not found, out of page")
        return line_positions_vertical[1] * scaling_factor, page_pdf.width


class LegalActPageRegionFooterNotes(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = get_scaling_factor(page_pdf, image_np)

        if len(line_positions) > 1 and line_positions[-1]:
            footer_x_start, footer_y, footer_width, _ = line_positions[-1]
            header_x_start, header_y, header_width, _ = line_positions[0]
            footer_x_start_tab[page_pdf.page_number] = footer_x_start * scaling_factor
            footer_width_tab[page_pdf.page_number] = footer_width * scaling_factor
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
            print(f"Error in page {page_pdf.page_number}: {e}")
            print(f"Using Vertical lines for {page_pdf.page_number}")
            vertical_line_positions = LegalActPageRegion.get_vertical_line_positions(image_np)
            if vertical_line_positions[0]* scaling_factor >= footer_x_start*scaling_factor - 13:
                top_y = LegalActPageRegionFooterNotes.get_top_y_using_vertical_lines(footer_y, scaling_factor)
                x_start, x_end = LegalActPageRegionFooterNotes.get_x_axis_using_vertical_lines(vertical_line_positions,
                                                                                               scaling_factor)
                y_end = LegalActPageRegionFooterNotes.get_bottom_y_using_vertical_lines(page_pdf)
                return LegalActPageRegionFooterNotes(
                    start_x=x_start - buffer,
                    end_x=x_end + buffer,
                    start_y=top_y,
                    end_y=y_end - buffer,
                    page_number=page_pdf.page_number
                )
        elif len(line_positions) == 1 and line_positions[0]:
            footer_x_start, footer_y, footer_width, _ = line_positions[0]
            vertical_line_positions = LegalActPageRegion.get_vertical_line_positions(image_np)
            if footer_y * scaling_factor > page_pdf.height * 0.15 and vertical_line_positions[0]*scaling_factor >= footer_x_start*scaling_factor - 13:
                top_y = LegalActPageRegionFooterNotes.get_top_y_using_vertical_lines(footer_y, scaling_factor)
                x_start, x_end = LegalActPageRegionFooterNotes.get_x_axis_using_vertical_lines(vertical_line_positions,
                                                                                               scaling_factor)
                y_end = LegalActPageRegionFooterNotes.get_bottom_y_using_vertical_lines(page_pdf)
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

    @staticmethod
    def get_top_y_using_vertical_lines(footer_y_start: float, scaling_factor: float) -> float:
        return footer_y_start * scaling_factor

    @staticmethod
    def get_bottom_y_using_vertical_lines(page_pdf: pdfplumber.pdf.Page) -> float:
        return LegalActPageRegionParagraphs.get_bottom_y_without_line(page_pdf)

    @staticmethod
    def get_x_axis_using_vertical_lines(line_positions_vertical: List[int], scaling_factor: float) -> Tuple[
        float, float]:
        if len(line_positions_vertical) != 2 or line_positions_vertical[1] <= line_positions_vertical[0]:
            raise ValueError("Line positions vertical not found, or have invalid values")
        return line_positions_vertical[0] * scaling_factor, line_positions_vertical[1] * scaling_factor


class LegalActPageRegionAttachment(LegalActPageRegion):

    @staticmethod
    def build(page_image: Image, page_pdf: pdfplumber.pdf.Page, buffer: int = 1) -> Union['LegalActPageRegion', None]:
        image_np = np.array(page_image.convert("RGB"))
        line_positions = LegalActPageRegion.get_line_positions(image_np)
        scaling_factor = get_scaling_factor(page_pdf, image_np)
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
        navbar_match = re.search(NAV_PATTERN, text)
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

    @staticmethod
    def get_top_y_using_vertical_lines(**kwargs) -> float:
        pass

    @staticmethod
    def get_bottom_y_using_vertical_lines(**kwargs) -> float:
        pass

    @staticmethod
    def get_x_axis_using_vertical_lines(**kwargs) -> Tuple[float, float]:
        pass


class AttachmentPageRegion:

    def __init__(self, start_x: float, start_y: float, end_x: float, end_y: float, page_number: int,
                 page_height: float):
        self.start_x = start_x
        self.start_y = start_y
        self.end_x = end_x
        self.end_y = end_y
        self.page_number = page_number
        self.page_height = page_height

    @staticmethod
    def build(start_x: float, start_y: float, end_x: float, end_y: float, page_number: int, page_height: float):
        return AttachmentPageRegion(
            start_x=start_x,
            end_x=end_x,
            end_y=end_y,
            start_y=start_y,
            page_number=page_number,
            page_height=page_height
        )


class AttachmentRegion:

    def __init__(self, page_regions: List[AttachmentPageRegion], name: str):
        self._page_regions = page_regions
        self._name = name

    @classmethod
    def build(cls, attachment_header_index: int, pdf: pdfplumber.pdf, attachments_names: List[str],
              attachments_page_regions: List[Union[LegalActPageRegion, None]], buffer: int = 1) -> 'AttachmentRegion':
        attachment_header = attachments_names[attachment_header_index]
        if attachment_header == 'Załącznik nr 9':
            r = 4

        next_attachment_header = attachments_names[
            attachment_header_index + 1] if attachment_header_index + 1 < len(attachments_names) else None
        attachment_start_y, attachment_start_page = AttachmentRegion.get_top_y_and_page_number(attachment_header,
                                                                                               pdf,
                                                                                               attachments_page_regions,
                                                                                               buffer)
        attachment_end_y, attachment_end_page = AttachmentRegion.get_bottom_y_and_page_number(
            next_attachment_header,
            pdf,
            attachments_page_regions,
            buffer)

        page_regions = []

        for page_num in range(attachment_start_page, attachment_end_page + 1):
            attachment_page_region = list(filter(lambda x: x.page_number == page_num, attachments_page_regions))[0]
            pdf_page = pdf.pages[page_num - 1]
            start_y = attachment_start_y if page_num == attachment_start_page else attachment_page_region.start_y
            end_y = attachment_end_y if page_num == attachment_end_page else attachment_page_region.end_y

            page_region = AttachmentPageRegion.build(attachment_page_region.start_x, start_y,
                                                     attachment_page_region.end_x, end_y, page_num, pdf_page.height)
            page_regions.append(page_region)
        return AttachmentRegion(page_regions=page_regions, name=attachment_header)

    @staticmethod
    def is_start_of_the_page(text_before_start: str):
        return text_before_start == '' or text_before_start.isspace()

    @staticmethod
    def get_top_y_and_page_number(attachment_header: str, pdf: pdfplumber.pdf,
                                  attachments_page_regions: List[Union[LegalActPageRegion, None]], buffer: int = 1) -> \
            Tuple[float, int]:
        for index, attachment_page_region in enumerate(attachments_page_regions):
            page = pdf.pages[attachment_page_region.page_number - 1]
            page_attachment_bbox = (attachment_page_region.start_x, attachment_page_region.start_y,
                                    attachment_page_region.end_x,
                                    attachment_page_region.end_y)
            words = page.within_bbox(page_attachment_bbox).extract_words()
            match_text = re.sub(r'[\s\n\r]+', ' ', attachment_header)
            for idx, word in enumerate(words):
                # Join words to compare with match_text, and check within a reasonable range
                text_before_start = ' '.join(w['text'] for w in words[:idx])
                candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(match_text.split())])
                if match_text == candidate_text and AttachmentRegion.is_start_of_the_page(text_before_start):

                    return attachment_page_region.start_y, attachment_page_region.page_number
                elif match_text == candidate_text:
                    return words[idx]['top'] - buffer, attachment_page_region.page_number

        raise ValueError(f"Attachment header {attachment_header} not found in the attachments page regions")

    @staticmethod
    def get_bottom_y_and_page_number(next_attachment_header: Union[str, None], pdf: pdfplumber.pdf,
                                     attachments_page_regions: List[Union[LegalActPageRegion, None]],
                                     buffer: int = 1) -> \
            Tuple[float, int]:
        if next_attachment_header is None:
            return attachments_page_regions[-1].end_y, attachments_page_regions[-1].page_number
        for index, attachment_page_region in enumerate(attachments_page_regions):
            page = pdf.pages[attachment_page_region.page_number - 1]
            page_attachment_bbox = (attachment_page_region.start_x, attachment_page_region.start_y,
                                    attachment_page_region.end_x,
                                    attachment_page_region.end_y)
            words = page.within_bbox(page_attachment_bbox).extract_words()

            match_text = re.sub(r'[\s\n\r]+', ' ', next_attachment_header)
            for idx, word in enumerate(words):
                # Join words to compare with match_text, and check within a reasonable range
                text_before_start = ' '.join(w['text'] for w in words[:idx])
                candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(match_text.split())])
                if match_text == candidate_text and AttachmentRegion.is_start_of_the_page(text_before_start):
                    if index < 1:
                        raise ValueError(
                            f"Invalid state ,Attachment header {next_attachment_header} not found in the attachments page regions")
                    return attachments_page_regions[index - 1].end_y, attachments_page_regions[index - 1].page_number
                elif match_text == candidate_text:
                    return words[idx]['top'], attachment_page_region.page_number

        raise ValueError(f"Attachment header {next_attachment_header} not found in the attachments page regions")

    @property
    def name(self) -> str:
        return self._name

    @property
    def page_regions(self) -> List[AttachmentPageRegion]:
        return self._page_regions

    def add_region(self, region: AttachmentPageRegion):
        """Adds a region to the page_regions list."""
        self._page_regions.append(region)


def get_scaling_factor(page: pdfplumber.pdf.Page, image: np.ndarray) -> float:
    """
    Calculate the scaling factor to convert pixel coordinates to PDF coordinates
    :param page: pdfplumber.pdf.Page object
    :param image: numpy.ndarray image of the page
    :return: float scaling factor
    """
    return page.height / image.shape[0]


def get_pages_with_attachments(pdf: pdfplumber.pdf.PDF) -> Set[int]:
    """
    Find pages with attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of page numbers with attachments
    """
    attachment_pages = set()
    for page_index, page in enumerate(pdf.pages):
        text = page.extract_text()
        matches = list(re.finditer(LEGAL_ANNOTATION_PATTERN, text, re.DOTALL))
        if matches and len(matches) > 0:
            for page_from_index in range(page_index, len(pdf.pages)):
                page_iter = pdf.pages[page_from_index]
                attachment_pages.add(page_iter.page_number)
            return attachment_pages
        else:
            matches = list(re.finditer(r"[\n\r]+Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s*[a-zA-Z\s\d]*[\n\r]+", text))
            if matches and len(matches) > 0:
                for page_from_index in range(page_index, len(pdf.pages)):
                    page_iter = pdf.pages[page_from_index]
                    attachment_pages.add(page_iter.page_number)
    return attachment_pages


def get_attachments_headers(pdf: pdfplumber.pdf.PDF, attachments_page_regions: List[Union[LegalActPageRegion, None]]) -> \
        List[str]:
    """
    Find headers of attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of attachment headers
    """
    attachment_headers = []
    for attachment_page_region in attachments_page_regions:
        page = pdf.pages[attachment_page_region.page_number - 1]
        page_attachment_bbox = (attachment_page_region.start_x, attachment_page_region.start_y,
                                attachment_page_region.end_x,
                                attachment_page_region.end_y)
        text_of_page_within_attachment = page.within_bbox(page_attachment_bbox).extract_text()
        matches = list(re.finditer(LEGAL_ANNOTATION_PATTERN, text_of_page_within_attachment, re.DOTALL))
        if len(matches) > 0:
            for match in matches:
                header = match.group(1).strip()
                if header in attachment_headers:
                    raise ValueError(f"Invalid state in finding attachments headers: {header} already found.")
                attachment_headers.append(header)
        else:
            matches = list(re.finditer(r"[\n\r]+(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k)\s*[a-zA-Z\s\d]*[\n\r]+",
                                       text_of_page_within_attachment, re.DOTALL))
            for match in matches:
                header = match.group(1).strip()
                if header in attachment_headers:
                    raise ValueError(f"Invalid state in finding attachments headers: {header} already found.")
                attachment_headers.append(header)
    return attachment_headers


def declare_part_of_legal_act(pdf_bytes: BytesIO):
    # Convert PDF bytes to images (one image per page)

    pages = convert_from_bytes(pdf_bytes.getvalue())
    attachments_page_regions = []
    page_regions_paragraphs = []
    page_regions_margin_notes = []
    page_regions_footer_notes = []
    attachments_regions = []
    with pdfplumber.open(BytesIO(pdf_bytes.getvalue())) as pdf:
        pages_with_attachments = get_pages_with_attachments(pdf)

        for index, page_image in enumerate(pages):
            pdf_page = pdf.pages[index]
            if pdf_page.page_number not in pages_with_attachments:
                page_region_text = LegalActPageRegionParagraphs.build(page_image, pdf_page)
                page_regions_paragraphs.append(page_region_text)
                bbox = (
                    page_region_text.start_x, page_region_text.start_y, page_region_text.end_x,
                    page_region_text.end_y)
                extract_text = pdf_page.within_bbox(bbox).extract_text()
                r = 4
                margin_notes = LegalActPageRegionMarginNotes.build(page_image, pdf_page)
                page_regions_margin_notes.append(margin_notes)
                bbox = (margin_notes.start_x, margin_notes.start_y, margin_notes.end_x, margin_notes.end_y)
                extract_text_margin = pdf_page.within_bbox(bbox).extract_text()
                footer_notes = LegalActPageRegionFooterNotes.build(page_image, pdf_page)
                #
                if footer_notes:
                    page_regions_footer_notes.append(footer_notes)
                    bbox = (footer_notes.start_x, footer_notes.start_y, footer_notes.end_x, footer_notes.end_y)
                    extract_text_footer = pdf_page.within_bbox(bbox).extract_text()
                    r = 4
            else:
                attachments_page_regions.append(LegalActPageRegionAttachment.build(page_image, pdf_page))
        attachment_headers = get_attachments_headers(pdf, attachments_page_regions)
        for attachment_header_index in range(len(attachment_headers)):
            attachment_region = AttachmentRegion.build(attachment_header_index, pdf, attachment_headers,
                                                       attachments_page_regions)
            attachments_regions.append(attachment_region)

    return page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions


def crop_pdf_attachments(pdf_content: str, datalake: Datalake, attachments_regions: List[AttachmentRegion]):
    pdf_reader = PdfFileReader(pdf_content)

    for i, attachment_region in enumerate(attachments_regions):
        pdf_writer = PdfFileWriter()
        for attachment_page_region in attachment_region.page_regions:
            page = pdf_reader.getPage(attachment_page_region.page_number - 1)
            page_height = attachment_page_region.page_height
            top_y = page_height - attachment_page_region.start_y
            bottom_y = page_height - attachment_page_region.end_y
            page.mediaBox.upperRight = (page.mediaBox.getUpperRight_x(), bottom_y)
            page.mediaBox.lowerLeft = (page.mediaBox.getLowerLeft_x(), top_y)
            pdf_writer.addPage(page)
        output_filename = f"pdfs/cropped_attachment_{attachment_region.name}.pdf"

        # Save to file
        with open(output_filename, "wb") as output_file:
            pdf_writer.write(output_file)

        # Optionally, if you want to save to memory and upload to datalake
        output_stream = BytesIO()
        pdf_writer.write(output_stream)
        output_stream.seek(0)


# def crop_pdf_attachments(pdf_content: str, datalake: Datalake, attachment_boundaries,
#                          pages_boundaries: List[Dict[str, Union[int, float]]],
#                          padding: int = 2):
#     pdf_reader = PdfFileReader(pdf_content)
#
#     for i, boundary in enumerate(attachment_boundaries):
#         pdf_writer = PdfFileWriter()
#
#         start_page = boundary['start_page']
#         end_page = boundary['end_page']
#         start_y = float(boundary['start_y'])
#         end_y = float(boundary['end_y'])
#
#         for page_num in range(start_page, end_page + 1):
#             page = pdf_reader.getPage(page_num)
#             page_height = pages_boundaries[page_num]['page_height']
#             navbar_y = pages_boundaries[page_num]['start_y']
#             footer_y = pages_boundaries[page_num]['end_y']
#
#             # Calculate the PDF coordinates
#             adjusted_start_y = page_height - start_y + padding if page_num == start_page and start_y > navbar_y else page_height - navbar_y + 3 * padding
#             adjusted_end_y = page_height - end_y if page_num == end_page and end_y < footer_y else page_height - footer_y - padding
#
#             page.mediaBox.upperRight = (page.mediaBox.getUpperRight_x(), adjusted_end_y)
#             page.mediaBox.lowerLeft = (page.mediaBox.getLowerLeft_x(), adjusted_start_y)
#
#             # Add the cropped page to the new PDF
#             pdf_writer.addPage(page)
#
#         datalake.save_to_datalake_pdf(pdf_writer=pdf_writer,
#                                       s3_key=f"stages/cropped-attachments/attachment_{i + 1}.pdf")
#         # Save the cropped section as a new PDF
#
#     print(f"Saved {len(attachment_boundaries)} cropped ")


pdf_s3_path = 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D20010639Lj.pdf'
# pdf_s3_path = 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D19830207Lj.pdf'
# pdf_s3_path = 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D19910031Lj.pdf'
# pdf_s3_path = 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D20060550Lj.pdf'

bucket, key = extract_bucket_and_key(pdf_s3_path)

datalake = Datalake(datalake_bucket=DATALAKE_BUCKET, aws_region=AWS_REGION)

bytes__kk = io.BytesIO(
    boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())

page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions = \
    declare_part_of_legal_act(bytes__kk)

# pages_boundaries = find_content_boundaries(datalake.read_from_datalake_pdf(key))
#
# # Example usage
# attachment_boudaries = extract_attachments(datalake.read_from_datalake_pdf(key), pages_boundaries=pages_boundaries)

# crop_pdf_attachments(
#     pdf_content=datalake.read_from_datalake_pdf(key),
#     datalake=datalake,
#     attachment_boundaries=attachment_boudaries,
#     pages_boundaries=pages_boundaries
# )

crop_pdf_attachments(pdf_content=bytes__kk, datalake=datalake, attachments_regions=attachments_regions)

client_sd = boto3.client('servicediscovery', region_name='eu-west-1')
client_ec2 = boto3.client('ec2', region_name='eu-west-1')

text = """
Art. 129. Minister właściwy do spraw gospodarki surowcami energetycznymi, w porozumieniu z ministrem właściwym do spraw finansów publicznych, może wyposażyć Zakład w mienie inne niż określone w art. 128.
Art. 130. (pominięty)
Art. 131. Pracownicy zatrudnieni w Zakładzie Doświadczalnym Unieszkodliwiania Odpadów Promieniotwórczych Instytutu Energii Atomowej stają się pracownikami Zakładu zgodnie z art. 231 Kodeksu pracy.
Art. 132. Minister Obrony Narodowej w odniesieniu do podległych mu jednostek organizacyjnych i minister właściwy do spraw wewnętrznych w odniesieniu do Policji, Państwowej Straży Pożarnej, Straży Granicznej i podległych mu jednostek organizacyjnych, po zasięgnięciu opinii Prezesa Agencji, określają, w drodze zarządzenia, sposób wykonywania przepisów ustawy w tych jednostkach.
Art. 133. 1. Główny Inspektor Dozoru Jądrowego i inspektorzy dozoru jądrowego, którzy zostali powołani lub uzyskali uprawnienia przed dniem wejścia w życie ustawy, stają się odpowiednio Głównym Inspektorem Dozoru Jądrowego i inspektorami dozoru jądrowego w rozumieniu ustawy.
2. Zezwolenia wydane na podstawie ustawy, o której mowa w art. 138, zachowują ważność do upływu terminu określonego w zezwoleniu.
3. Uprawnienia uzyskane na podstawie art. 33 ust. 3 pkt 1 oraz ust. 4 ustawy, o której mowa w art. 138, zachowują ważność do upływu terminu określonego w uprawnieniu.
4. Zezwolenia związane z nabywaniem i stosowaniem substancji promieniotwórczych, wydane na podstawie przepisów obowiązujących przed dniem wejścia w życie ustawy, o której mowa w art. 138, w szczególności na podstawie przepisów:
1) rozporządzenia Rady Ministrów z dnia 18 czerwca 1968 r. w sprawie bezpieczeństwa i higieny pracy przy stosowaniu promieniowania jonizującego (Dz. U. poz. 122),
2) uchwały nr 266/64 Rady Ministrów z dnia 29 sierpnia 1964 r. w sprawie użytkowania substancji promieniotwórczych,

Załacznik nr  3
SYMBOL PROMIENIOWANIA JONIZUJĄCEGO'
Załącznik nr  4 
 
DAWKI GRANICZNE PROMIENIOWANIA JONIZUJĄCEGO  
 
I. Dla pracowników  
 
1. Dla pracowników dawka graniczna, wyrażona jako dawka skuteczna (efektywna), wynosi 20  mSv w  ciągu roku 
kalendarzowego.  
2. W przypadku wyrażenia zgody, o  której mowa w  art. 14 ust. 1a ustawy, dawka, o  której mowa w  pkt 1, może być w  danym  
roku kalendarzowym przekroczona do wartości 50  mSv, o  ile zgodę na takie przekroczenie wyda, ze względu na szczególne  
warunki lub okoliczności wykonywania działalności związanej z  narażeniem, organ właściwy do wydania zezwolenia, 
przyjęcia zgłoszenia albo przyjęcia powiadomienia, o  których mowa w  art. 4 ust. 1 lub 1a ustawy.  
3. W  przypadku, o  którym mowa w  pkt 2, średnia roczna dawka skuteczna (efektywna) w  każdym okresie pięciu kolejnych 
lat kalendarzowych, w  tym lat, w  których dawka graniczna została przekroczona, nie może przekroczyć 20  mSv.  
4. Dawka graniczna, wyrażona jako dawka równoważna, wynosi w  ciągu roku kalendarzowego:  
1)  20 mSv – dla soczewki oka;  
2)  500 mSv – dla skóry, jako wartość średnia dla każdego obszaru 1  cm2 napromienionej części skóry, niezależnie od 
napromienionej powierzchni;  
3)  500 mSv – dla kończyn zdefiniowanych jako dłonie, przedramiona, stopy i  kostki.  
5. Wartość, o  której mowa w  pkt 4 ppkt 1, może być w  pojedynczym roku kalendarzowym przekroczona do 50  mSv, jednakże  
w każdym okresie pięciu kolejnych lat kalendarzowych, w  tym lat, w  których dawka graniczna została przekroczona, nie 
może przekroczyć 100  mSv.   
 
II. Dla uczniów, studentów i  praktykantów  
 
1. Dla uczniów, studentów i  praktykantów, w  wieku 18  lat i powyżej, mają zastosowanie dawki graniczne ustalone w  poz. I. 
2. Dla uczniów, studentów i  praktykantów, w  wieku od 16  lat do 18  lat, dawka graniczna, wyrażona jako dawka skuteczna 
(efektywna), z  zastrzeżeniem art.  14 ust. 1b i 1c ustawy, wynosi 6  mSv w  ciągu roku kalendarzowego, przy czym dawka 
graniczna, wyrażona jako dawk a równoważna, wynosi w  ciągu roku kalendarzowego:  
1)  15 mSv – dla soczewki oka;  
2)  150 mSv – dla skóry, jako wartość średnia dla każdego obszaru 1  cm2 napromienionej części skóry, niezależnie od 
napromienionej powierzchni;  
3)  150 mSv – dla kończyn zdefiniowanych jako dłonie, przedramiona, stopy i  kostki.  
3. Dla uczniów, studentów i  praktykantów, w  wieku poniżej 16  lat, mają zastosowanie dawki graniczne ustalone w  poz. III. 
 
III. Dla osób z  ogółu ludności
Załącznik nr  5 
 
KWALIFIKACJA DZIAŁALNOŚCI ZWIĄZANEJ Z  NARAŻENIEM DO KATEGORII ZAGROŻEŃ  
 
1. Kategoria I  – działalności związane z  narażeniem mogące prowadzić do wystąpienia na terenie jednostki organizacyjnej 
zdarzenia radiacyjnego skutkującego lub mogącego skutkować poważnymi efektami deterministycznymi poza terenem tej 
jednostki, uzasadniającymi uruchomienie wyprzedzających działań interwencyjnych, w tym ewakuacji, nakazu pozostania 
w pomieszczeniach zamkniętych, podania preparatów ze stabilnym jodem oraz innych pilnych działań interwencyjnych.  
 
Kategoria I  obejmuje rozruch, eksploatację i  likwidację obiektu jądrowego, takiego jak reaktor o  mocy cieplnej powyżej 
100 MW (megawatów) lub przechowalnik zawierający wypalone paliwo jądrowe w  ilości równoważnej rdzeniowi reaktora 
o mocy cieplnej 3000  MW.  
 
2. Kategoria II – działalności związane z  narażeniem mogące prowadzić do wystąpienia na terenie jednostki organizacyjnej 
zdarzenia radiacyjnego skutkującego lub mogącego skutkować efektami stochastycznymi narażenia osób z  ogółu ludności 
poza terenem tej je dnostki uzasadniającymi uruchomienie pilnych działań interwencyjnych.  
 
Kategoria II obejmuje:  
1)  rozruch, eksploatację i  likwidację obiektu jądrowego, takiego jak reaktor o  mocy cieplnej powyżej 2  MW do 100  MW,  
przechowalnik zawierający wypalone paliwo jądrowe wymagające aktywnego chłodzenia, zakład wzbogacania izotopowego , 
zakład wytwarzania paliwa jądrowego lub zakład przerobu wypalonego paliwa jądrowego;  
2)  eksploatację lub zamknięcie składowiska odpadów promieniotwórczych.  
 
3. Kategoria III – działalności związane z  narażeniem mogące prowadzić wyłącznie do wystąpienia zdarzenia radiacyjnego 
powodującego zagrożenie jednostki organizacyjnej, uzasadniającego uruchomienie pilnych działań interwencyjnych na terenie  
jednostki organizacyjnej.
Załącznik nr  5 
 
KWALIFIKACJA DZIAŁALNOŚCI ZWIĄZANEJ Z  NARAŻENIEM DO KATEGORII ZAGROŻEŃ  
 
1. Kategoria I  – działalności związane z  narażeniem mogące prowadzić do wystąpienia na terenie jednostki organizacyjnej 
zdarzenia radiacyjnego skutkującego lub mogącego skutkować poważnymi efektami deterministycznymi poza terenem tej 
jednostki, uzasadniającymi uruchomienie wyprzedzających działań interwencyjnych, w tym ewakuacji, nakazu pozostania 
w pomieszczeniach zamkniętych, podania preparatów ze stabilnym jodem oraz innych pilnych działań interwencyjnych.  
 
Kategoria I  obejmuje rozruch, eksploatację i  likwidację obiektu jądrowego, takiego jak reaktor o  mocy cieplnej powyżej 
100 MW (megawatów) lub przechowalnik zawierający wypalone paliwo jądrowe w  ilości równoważnej rdzeniowi reaktora 
o mocy cieplnej 3000  MW.  
 
2. Kategoria II – działalności związane z  narażeniem mogące prowadzić do wystąpienia na terenie jednostki organizacyjnej 
zdarzenia radiacyjnego skutkującego lub mogącego skutkować efektami stochastycznymi narażenia osób z  ogółu ludności 
poza terenem tej je dnostki uzasadniającymi uruchomienie pilnych działań interwencyjnych.  
 
Kategoria II obejmuje:  
1)  rozruch, eksploatację i  likwidację obiektu jądrowego, takiego jak reaktor o  mocy cieplnej powyżej 2  MW do 100  MW,  
przechowalnik zawierający wypalone paliwo jądrowe wymagające aktywnego chłodzenia, zakład wzbogacania izotopowego , 
zakład wytwarzania paliwa jądrowego lub zakład przerobu wypalonego paliwa jądrowego;  
2)  eksploatację lub zamknięcie składowiska odpadów promieniotwórczych.  
 
3. Kategoria III – działalności związane z  narażeniem mogące prowadzić wyłącznie do wystąpienia zdarzenia radiacyjnego 
powodującego zagrożenie jednostki organizacyjnej, uzasadniającego uruchomienie pilnych działań interwencyjnych na terenie  
jednostki organizacyjnej.

"""

pattern = r"(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+n\s*r\s+\d+\s*?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+n\s*r\s+\d+)|$)"

ss = re.findall(pattern, text, re.DOTALL)

text_without_attachments = re.sub(pattern, '', text, flags=re.DOTALL).strip()

for i, (header, content) in enumerate(ss):
    print(f"Header {i}: {header}")
    print(f"Content {i}: {content}")
    ww = header + " " + content
    if re.search(r'(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+n\s*r\s+\d+)', ww, re.DOTALL):
        print("Found")
    w = 4

import boto3

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# def get_row_metadata_of_documents_each_year(year, type_document, invoke_id):
#     w = f"{SEJM_API_URL}/{type_document}/{year}"
#     response = make_api_call(f"{SEJM_API_URL}/{type_document}/{year}")
#     if 'items' not in response:
#         raise Exception(
#             f"There is no 'documents' key in the response from the Sejm API, for year: {year} and type: {type_document}")
#
#     json_data_list = response['items']
#
#     # Create a DataFrame
#     df = pd.DataFrame(json_data_list)
#
#     # Define the schema explicitly to enforce consistent data types
#     schema = {
#         'ELI': 'str',
#         'year': 'int',
#         'status': 'str',
#         'announcementDate': 'str',
#         'volume': 'int',
#         'address': 'str',
#         'displayAddress': 'str',
#         'promulgation': 'str',
#         'pos': 'int',
#         'publisher': 'str',
#         'changeDate': 'str',
#         'textHTML': 'bool',
#         'textPDF': 'bool',
#         'title': 'str',
#         'type': 'str'
#     }
#     # Define the S3 path to store the Parquet file
#     s3_key = f"api-sejm/test2/{type_document}/{year}.parquet"
#
#     # Write the Parquet file to S3
#     s3_client = boto3.client('s3', region_name='eu-west-1')
#     out_buffer = BytesIO()
#
#     # Reindex the DataFrame to ensure missing columns are filled with None/NaN
#     df = df.reindex(columns=meta_DULegalDocumentsMetaData.columns)
#
#     # Convert DataFrame to Apache Arrow Table
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, out_buffer)
#     out_buffer.seek(0)
#
#     s3_client.upload_fileobj(out_buffer, DATALAKE_BUCKET, s3_key)
#
# def make_api_call_html(url):
#     try:
#         session_requests = requests.Session()
#         session_requests.mount(url, HTTPAdapter(max_retries=10))
#
#         response = session_requests.get(url, timeout=10)
#         response.raise_for_status()
#         if 'text/html' not in response.headers['Content-Type']:
#             raise Exception(f"Content-Type is not 'text/html' for url: {url}")
#
#         return response.text.strip()
#     except requests.exceptions.Timeout as timeout_err:
#         raise Exception(f"Request timed out while calling the Sejm API for url: {url}")
#     except requests.exceptions.HTTPError as http_err:
#         raise Exception("Error occurred while calling the Sejm API")
#     except Exception as err:
#         raise Exception("Error occurred while calling the Sejm API")
#
#
# def get_filename_for_legal_act_row(row) -> Union[str, None]:
#     data = make_api_call(f"{SEJM_API_URL}/{row['ELI']}")
#
#     # Check if there's a file with type 'U' in the 'texts' field
#
#     ## TODO ADD checking text of html
#     if 'texts' in data and 'textHTML' in data:
#
#         for text in data['texts']:
#             if text.get('type') == 'H':
#                 fileNameHTML =  text.get('fileName')
#                 data_html = make_api_call_html(f"{SEJM_API_URL}/{row['ELI']}/{fileNameHTML}")
#
#                 r = 4
#
#     if row['status'] == 'akt posiada tekst jednolity':
#         print("dddd")
#         # raise FlowStepError(
#         #     f"Missing file with type 'U' for ELI: {row['ELI']}, but for status 'akt posiada tekst jednolity' we always expect a 'U' type file.")
#     return None
#
#
#
# get_filename_for_legal_act_row({'ELI': 'DU/1993/34'})
#
# get_row_metadata_of_documents_each_year(1961, 'DU', '11111')
#
# client = Client('localhost:8786')
#
# r = 's3://datalake-bucket-123/api-sejm/test2/DU/*.parquet'
#
# ddf_from_parquet = dd.read_parquet(r,
#                                    engine='auto',
#                                    storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
#                                    )
# ddf_from_parquet = ddf_from_parquet.astype(
#     {col: dtype.name for col, dtype in meta_DULegalDocumentsMetaData_without_filename.dtypes.to_dict().items()})
#
# w = ddf_from_parquet.compute()
#
# t = 4

# uri = "mongo_db+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongo_db.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"

# Create a new client and connect to the server
# mongo_client = MongoClient(uri, server_api=ServerApi('1'))
#
# # Send a ping to confirm a successful connection
# ## TODO it does not work
# try:
#     mongo_client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)
#
#
#
# ll = 4
#
# document_schema = {
#     "bsonType": "object",
#     "required": [
#         "general_info",  # The embedded document for all meta-information
#         "invoke_id",     # UUID as a string
#         "pages",         # Page number and content
#         "expires_at"     # Expiry date for TTL
#     ],
#     "properties": {
#         "general_info": {  # Embedded document containing meta-information
#             "bsonType": "object",
#             "required": ["ELI", "document_year", "status", "address", "displayAddress", "promulgation", "pos", "title", "type", "filename_of_pdf"],
#             "properties": {
#                 "ELI": {
#                     "bsonType": "string",
#                     "description": "Must be a string and is required."
#                 },
#                 "document_year": {
#                     "bsonType": "int",
#                     "description": "Must be an integer representing the document's year."
#                 },
#                 "status": {
#                     "bsonType": "string",
#                     "description": "Must be a string describing the document's status."
#                 },
#                 "address": {
#                     "bsonType": "string",
#                     "description": "Must be a string representing the address."
#                 },
#                 "displayAddress": {
#                     "bsonType": "string",
#                     "description": "Must be a string for the display address."
#                 },
#                 "promulgation": {
#                     "bsonType": "string",
#                     "description": "Must be a string representing the promulgation date."
#                 },
#                 "pos": {
#                     "bsonType": "int",
#                     "description": "Must be an integer representing the position."
#                 },
#                 "title": {
#                     "bsonType": "string",
#                     "description": "Must be a string representing the title of the document."
#                 },
#                 "type": {
#                     "bsonType": "string",
#                     "description": "Must be a string representing the type of document."
#                 },
#                 "filename_of_pdf": {
#                     "bsonType": "string",
#                     "description": "Must be a string representing the PDF filename."
#                 }
#             }
#         },
#         "invoke_id": {
#             "bsonType": "string",  # UUID as a string
#             "description": "Must be a string representing the UUID and is required."
#         },
#         "pages": {  # Pages is a dictionary, where each page number maps to its text
#             "bsonType": "object",
#             "additionalProperties": {
#                 "bsonType": "string",
#                 "description": "The text of each page, stored as a dictionary where keys are page numbers."
#             }
#         },
#         "expires_at": {
#             "bsonType": "date",
#             "description": "The date the document expires. MongoDB will use this for TTL deletion."
#         }
#     }
# }
#
#
# preprocessing_legal_acts_texts_db = mongo_client["preprocessing_legal_acts_texts"]
#
#
# cmd_extract_text_and_table_page_number_stage_collection = {
#     "collMod": "extract_text_and_table_page_number_stage",
#     "validator": {"$jsonSchema": document_schema},
#     "validationLevel": "strict"  # Enforce strict validation
# }
#
# # Apply the schema to the collection
# preprocessing_legal_acts_texts_db.command(cmd_extract_text_and_table_page_number_stage_collection)
#
#
# extract_text_and_table_page_number_stage_collection = preprocessing_legal_acts_texts_db["extract_text_and_table_page_number_stage"]
#
#
#
#
# extract_text_and_table_page_number_stage_collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
#
#
# extract_text_and_table_page_number_stage_collection.create_index([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
#
#
# query = {
#     "general_info.ELI": "DU/2023/1",  # Replace with the actual ELI you want to query
#     "invoke_id": "4334344555"               # Replace with the actual invoke_id you want to query
# }
#
# try:
#     document = extract_text_and_table_page_number_stage_collection.find_one(query)
#     if document:
#         print(f"Found document: {document}")
#     else:
#         print("No document found with the given ELI and invoke_id.")
# except Exception as e:
#     print(f"Error querying document: {e}")

@retry_dask_task(retries=3, delay=5)
def calc(x):
    a = x + 5
    raise Exception("Error")


def create_log_stream_if_not_exists(client, log_group_name, log_stream_name):
    existing_log_streams = client.describe_log_streams(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name)
    if not any(log_stream['logStreamName'] == log_stream_name for log_stream in existing_log_streams['logStreams']):
        client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        print(f"Created log stream: {log_stream_name}")


# Step 3: Send logs to CloudWatch
def send_log_event(client, log_group_name, log_stream_name, message):
    timestamp = int(round(time.time() * 1000))  # CloudWatch Logs expect timestamp in milliseconds

    # Get the latest sequence token if available
    response = client.describe_log_streams(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name)
    log_streams = response['logStreams']

    sequence_token = None
    if 'uploadSequenceToken' in log_streams[0]:
        sequence_token = log_streams[0]['uploadSequenceToken']

    # Send log event
    log_event = {
        'logGroupName': log_group_name,
        'logStreamName': log_stream_name,
        'logEvents': [
            {
                'timestamp': timestamp,
                'message': message
            }
        ]
    }

    # Add sequence token if available
    if sequence_token:
        log_event['sequenceToken'] = sequence_token

    response = client.put_log_events(**log_event)
    print(f"Log event sent: {message}")


def send_log(log: str):
    logs_client = boto3.client('logs', region_name='eu-west-1')

    send_log_event(logs_client, "my-log-group", "k", log)


# logger = dask_worker_logger(stream_name="first-stream")


r = 4


def get_mongodb_collection(db_name, collection_name):
    ## localhost
    uri = "mongo_db+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongo_db.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
    # uri = "mongo_db+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongo_db.net/"

    mongo_client = MongoClient(uri, server_api=ServerApi('1'))
    mongo_db = mongo_client[db_name]
    if db_name == "preprocessing_legal_acts_texts":
        if collection_name == "extract_text_and_table_page_number_stage":
            return MongodbCollection(
                collection_name="extract_text_and_table_page_number_stage",
                db_instance=mongo_db,
                collection_schema=extract_text_and_table_page_number_stage_schema,
                indexes=[
                    MongodbCollectionIndex([("expires_at", ASCENDING)], expireAfterSeconds=0),
                    MongodbCollectionIndex([("general_info.ELI", ASCENDING), ("invoke_id", ASCENDING)], unique=True)
                ]
            )
    raise Exception("Invalid db_name or collection_name Mongodb error")


extract_text_and_table_page_number_stage_collection = get_mongodb_collection(
    db_name="preprocessing_legal_acts_texts",
    collection_name="extract_text_and_table_page_number_stage"
)

r = 4

document_1 = {
    "general_info": {
        "ELI": "DU/1998/930",
        "document_year": 1998,
        "status": "akt posiada tekst jednolity",
        "announcementDate": None,
        "volume": 144,
        "address": "WDU19981440930",
        "displayAddress": "Dz.U. 1998 nr 144 poz. 930",
        "promulgation": None,
        "pos": 930,
        "publisher": "DU",
        "changeDate": "2024-07-09T13:21:30",
        "textHTML": True,
        "textPDF": True,
        "title": "Ustawa z dnia 20 listopada 1998 r. o zryczałtowanym podatku dochodowym od niektórych przychodów osiąganych przez osoby fizyczne.",
        "type": "Ustawa",
        "filename_of_pdf": "D19980930Lj.pdf",
        "s3_pdf_path": "s3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D19980930Lj.pdf"
    },
    "invoke_id": "222",  # UUID for invoke_id
    "pages": {
        "page_1": "Text for page 1",
        "page_2": "Text for page 2"
    },
    "pages_with_tables": [["1"], ["2"]],
    "expires_at": datetime.utcnow() + timedelta(hours=1)  # Expires in 30 days
}

try:
    extract_text_and_table_page_number_stage_collection.insert_one(document_1)
    print(f"Document inserted with invoke_id: {document_1['invoke_id']}")
except Exception as e:
    print(f"Error inserting document: {e}")

r = 4


def get_stack_output(stack_name, output_key):
    # Initialize the CloudFormation client
    cloudformation_client = boto3.client('cloudformation', region_name='eu-west-1')

    try:
        # Describe the CloudFormation stack
        response = cloudformation_client.describe_stacks(StackName=stack_name)

        # Get the outputs of the stack
        outputs = response['Stacks'][0].get('Outputs', [])

        # Look for the specific output key
        for output in outputs:
            if output['OutputKey'] == output_key:
                return output['OutputValue']

        # Raise an exception if the key is not found
        raise Exception(f"Output key '{output_key}' not found in the stack '{stack_name}'")

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


# Usage
# stack_name = "dask-stack-8d08edfd-2060-41c4-a7ad-0f198ab39e75"  # Your CloudFormation stack name
# output_key = "DaskSchedulerALBURL"  # The key you are looking for
#
# scheduler_url = get_stack_output(stack_name, output_key)
# if scheduler_url:
#     print(f"Dask Scheduler URL: {scheduler_url}")


# Replace with your service discovery details
# namespace_name = 'local-dask'
# service_name = 'Dask-Scheduler'
#
# # Get the namespace ID
# response = client_sd.list_namespaces(
#     Filters=[{'Name': 'TYPE', 'Values': ['DNS_PRIVATE']}]
# )
#
# namespace_id = None
# for namespace in response['Namespaces']:
#     if namespace['Name'] == namespace_name:
#         namespace_id = namespace['Id']
#         break
#
# if not namespace_id:
#     raise Exception(f"Namespace {namespace_name} not found")
#
#
# # Get the service ID
# response = client_sd.list_services(
#     Filters=[{'Name': 'NAMESPACE_ID', 'Values': [namespace_id]}]
# )
#
# service_id = None
# for service in response['Services']:
#     if service['Name'] == service_name:
#         service_id = service['Id']
#         break
#
# if not service_id:
#     raise Exception(f"Service {service_name} not found in namespace {namespace_name}")
#
# # Get the IP address of the Dask Scheduler service
# response = client_sd.discover_instances(
#     NamespaceName=namespace_name,
#     ServiceName=service_name,
#     MaxResults=1
# )
#
# if not response['Instances']:
#     raise Exception(f"No instances found for service {service_name}")
#
# # Assuming that the EC2 instance ID is passed as an attribute, such as `AWS_INSTANCE_ID`
# l = response['Instances'][0]['Attributes']
# scheduler_instance_id = response['Instances'][0]['InstanceId']
# print(f"Found EC2 instance ID: {scheduler_instance_id}")
#
# # Retrieve the public IP of the EC2 instance using the instance ID
# ec2_response = client_ec2.describe_instances(InstanceIds=[scheduler_instance_id])
# scheduler_public_ip = ec2_response['Reservations'][0]['Instances'][0]['PublicIpAddress']
#
# print(f"Found Dask Scheduler Public IP: {scheduler_public_ip}")
#
# # Connect to the Dask Scheduler using the public IP address
# scheduler_port = 8786  # Default Dask scheduler port
# client = Client(f'Dask-ALB-d74872113a831fd6.elb.eu-west-1.amazonaws.com:8786')
# client = Client('observant-bulldog-TCP-9e0edeeefad1641c.elb.eu-west-1.amazonaws.com:8786')
# client = Client('localhost:8786')
# print(client)

def get_row_metadata_of_documents_each_year(year, type_document, invoke_id):
    w = f"{SEJM_API_URL}/{type_document}/{year}"
    response = make_api_call(f"{SEJM_API_URL}/{type_document}/{year}")
    if 'items' not in response:
        raise Exception(
            f"There is no 'documents' key in the response from the Sejm API, for year: {year} and type: {type_document}")

    json_data_list = response['items']
    # for obj in json_data_list:
    #     for key, value in obj.items():
    #         if isinstance(value, bytes):
    #             obj[key] = value.decode('utf-8')

    df = pd.DataFrame(json_data_list)

    json_lines = df.to_json(orient='records', lines=True, force_ascii=False)

    upload_json_to_s3(json_lines, DATALAKE_BUCKET, f"api-sejm/{str(invoke_id)}/{type_document}/{year}")


# kk = get_row_metadata_of_documents_each_year(2019, 'DU', '11111')

client = Client('localhost:8786')

rrrr = 's3://datalake-bucket-123/stages/$ee53c0e0-f585-4c86-99dd-82c3336fc438/filter_rows_from_api_and_upload_to_parquet_for_further_processing/rows_to_update_dynamodb.parquet.gzip'

ddf_from_parquet = dd.read_parquet(rrrr, engine='auto', storage_options=get_storage_options_for_ddf_dask(AWS_REGION))

res = ddf_from_parquet.compute()


## for test
# ddf_from_parquet = ddf_from_parquet.compute()
# new_row = {'ELI': 'DU/1989/406'}
# new_row2 = {'ELI': 'DU/1989/2'}
# new_row3 = {'ELI': 'DU/1989/12'}
# new_rows4 = {'ELI': 'DU/2008/24'}
# new_rows5 = {'ELI': 'DU/1985/60'}
#
#
# new_row_df = pd.DataFrame([new_row, new_row2, new_row3, new_rows4, new_rows5])
#
# new_row_ddf = dd.from_pandas(new_row_df, npartitions=1)
# ddf_from_parquet = dd.concat([ddf_from_parquet, new_row_ddf],axis=0)

def replace_nan_with_defaults(df):
    # Replace NaN in string columns with empty strings
    df = df.fillna({
        col: '' for col, dtype in df.dtypes.items() if dtype == 'object'  # string columns
    })

    # Replace NaN in numeric columns with 0
    df = df.fillna({
        col: 0 for col, dtype in df.dtypes.items() if dtype in ['int64', 'float64']  # numeric columns
    })

    # Replace NaN in boolean columns with False
    df = df.fillna({
        col: False for col, dtype in df.dtypes.items() if dtype == 'bool'  # boolean columns
    })
    df['textHTML'] = df['textHTML'].astype(str)
    df['textPDF'] = df['textPDF'].astype(str)
    return df


# ddf_from_parquet = ddf_from_parquet.map_partitions(replace_nan_with_defaults)

lll = ddf_from_parquet.compute()

#
# meta = pd.DataFrame({
#     'ELI': pd.Series(dtype='str'),
#     'document_year': pd.Series(dtype='int'),
#     'status': pd.Series(dtype='str'),
#     'announcementDate': pd.Series(dtype='str'),
#     'volume': pd.Series(dtype='int'),
#     'address': pd.Series(dtype='str'),
#     'displayAddress': pd.Series(dtype='str'),
#     'promulgation': pd.Series(dtype='str'),
#     'pos': pd.Series(dtype='int'),
#     'publisher': pd.Series(dtype='str'),
#     'changeDate': pd.Series(dtype='str'),
#     'textHTML': pd.Series(dtype='bool'),
#     'textPDF': pd.Series(dtype='bool'),
#     'title': pd.Series(dtype='str'),
#     'type': pd.Series(dtype='str')
# })
#
# ddf_from_parquet.to_parquet(rrrr, engine='auto', compression='snappy', write_index=False,
#                        storage_options=get_storage_options_for_ddf_dask(AWS_REGION))


ddf_from_parquet_compute = ddf_from_parquet[['ELI']]


def process_acts(row, key):
    list_of_changes = []
    if key in row['call_result']:
        for act in row['call_result'][key]:
            act_of_change = {}
            if (
                    key == 'Akty zmieniające' or key == 'Akty uchylające' or key == 'Uchylenia wynikające z') and 'date' not in act:
                raise Exception(f"Missing 'date' key in the act of change: {act}, with {key} change, api error")
            if 'act' not in act or 'ELI' not in act['act'] or 'type' not in act['act'] or 'title' not in act['act']:
                raise Exception(f"Missing 'act' key in the act of change: {act}, api error")
            if key == 'Akty zmieniające' or key == 'Akty uchylające' or key == 'Uchylenia wynikające z':
                act_of_change['date_of_change_annotation'] = act['date']
            elif 'promulgation' in act['act']:
                act_of_change['date_of_change_annotation'] = act['act']['promulgation']
            elif act['act']['type'] == 'Obwieszczenie':
                act_of_change['date_of_change_annotation'] = act['act']['announcementDate']
            else:
                raise Exception(f"There is no date for change in the act of change: {act}, for key: {key} api error")
            act_of_change['ELI_annotation'] = act['act']['ELI']
            act_of_change['type_annotation'] = act['act']['type']
            act_of_change['title_annotation'] = act['act']['title']
            act_of_change['TYPE_OF_CHANGE'] = key
            list_of_changes.append(act_of_change)
    return list_of_changes


def extract_act_info(row):
    acts = process_acts(row, 'Akty zmieniające') + process_acts(row, 'Inf. o tekście jednolitym') \
           + process_acts(row, 'Akty uchylające') + process_acts(row, 'Uchylenia wynikające z')  ## to test this

    return acts if acts else np.nan


def call_api(row):
    # Construct the API request based on the row data
    api_url = f"{SEJM_API_URL}/{row['ELI']}/references"
    response = make_api_call(api_url)

    # Assuming the response is in JSON format, return it
    return response


def explode_and_normalize_partition(df, columns_order):
    df_filtered_from_empty_references = df[df['call_result_list'].notna()]
    df_exploded = df_filtered_from_empty_references.explode('call_result_list', ignore_index=True)
    df_normalized = pd.json_normalize(df_exploded['call_result_list'])
    df_final = pd.concat([df_exploded.drop(columns=['call_result_list', 'call_result']), df_normalized], axis=1)
    return df_final.reindex(columns=columns_order, fill_value=pd.NA)


ddf_from_parquet_compute['call_result'] = ddf_from_parquet_compute.apply(call_api, axis=1,
                                                                         meta=('call_result', 'object'))

meta_act_references = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'ELI_annotation': pd.Series(dtype='str'),
    'type_annotation': pd.Series(dtype='str'),
    'title_annotation': pd.Series(dtype='str'),
    'TYPE_OF_CHANGE': pd.Series(dtype='str'),
    'date_of_change_annotation': pd.Series(dtype='str'),
})

ddf_from_parquet_compute['call_result_list'] = ddf_from_parquet_compute.apply(extract_act_info, axis=1)

l = meta_act_references.columns.tolist()

columns_order_for_references = ['ELI', 'ELI_annotation', 'type_annotation', 'title_annotation', 'TYPE_OF_CHANGE',
                                'date_of_change_annotation']

df_final_api_ref = ddf_from_parquet_compute.map_partitions(explode_and_normalize_partition,
                                                           columns_order=meta_act_references.columns.tolist(),
                                                           meta=meta_act_references)

df_final_api_ref_computed = df_final_api_ref.compute()


def insert_partition_to_dynamodb(df_partition, table_name):
    boto_session = boto3.Session(region_name=AWS_REGION)
    dynamodb = boto_session.resource('dynamodb')
    documents_metadata_table = dynamodb.Table(table_name)
    # Insert rows into DynamoDB
    with documents_metadata_table.batch_writer() as batch:
        for _, row in df_partition.iterrows():
            try:
                batch.put_item(Item=row.to_dict())
            except Exception as e:
                print(f"Error inserting row: {row.get('id', 'unknown id')}, error: {e}")


df_final_api_ref.map_partitions(insert_partition_to_dynamodb, table_name="DULegalDocumentsMetaDataChanges_v2",
                                meta=meta_act_references)

total_segments_l = 10  # Number of segments to split the scan across


def scan_dynamodb_segment(segment, table_name, aws_region, total_segments, columns_order):
    boto_session = boto3.Session(region_name=aws_region)
    dynamodb = boto_session.resource('dynamodb')
    documents_metadata_table = dynamodb.Table(table_name)
    response = documents_metadata_table.scan(
        Segment=segment,
        TotalSegments=total_segments
    )

    items = response['Items']

    # Handle pagination if necessary
    while 'LastEvaluatedKey' in response:
        response = documents_metadata_table.scan(
            Segment=segment,
            TotalSegments=total_segments,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    # Return the result as a Pandas DataFrame
    return pd.DataFrame(items).reindex(columns=columns_order, fill_value=pd.NA)


# Create a Dask DataFrame containing segment numbers (0 to total_segments-1)
segments = list(range(total_segments_l))
ddf_segments = dd.from_pandas(pd.DataFrame({'segment': segments}), npartitions=total_segments_l)


def fetch_segment_data(df_partition, table_name, aws_region, total_segments, columns_order):
    segment = int(df_partition['segment'].iloc[0])  # Get the segment number
    return scan_dynamodb_segment(segment, table_name, aws_region, total_segments, columns_order)


meta = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'document_year': pd.Series(dtype='int'),
    'status': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'address': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'promulgation': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'publisher': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str')
})
kkk = "DULegalDocumentsMetaData"
# Fetch the data from DynamoDB in parallel using Dask

dynamodb_ddf_for_references = ddf_segments.map_partitions(
    lambda df: fetch_segment_data(df, "DULegalDocumentsMetaDataChanges_v2", AWS_REGION, 10,
                                  meta_act_references.columns.tolist()), meta=meta_act_references)

#### TODO run this code
merged_ddf_for_ref = df_final_api_ref.merge(
    dynamodb_ddf_for_references,
    on=['ELI', 'ELI_annotation'],
    how='left',
    suffixes=('_api', '_db')
)

merged_ddf_for_ref_outer = df_final_api_ref.merge(
    dynamodb_ddf_for_references,
    on=['ELI', 'ELI_annotation'],
    how='outer',
    suffixes=('_api', '_db'),
    indicator=True
)

merged_ddf_for_ref_computed = merged_ddf_for_ref.compute()
merged_ddf_for_ref_computed_outer = merged_ddf_for_ref_outer.compute()

new_rows_in_dynamodb = merged_ddf_for_ref_computed_outer[
    merged_ddf_for_ref_computed_outer['_merge'] == 'right_only'
    ]
new_rows_in_api = merged_ddf_for_ref_outer[
    merged_ddf_for_ref_outer['_merge'] == 'left_only'
    ]

ddf_new_rows_from_api = new_rows_in_api.rename(
    columns={col: col.replace('_api', '') for col in new_rows_in_api.columns if col.endswith('_api')}
)

ddf_new_rows_from_api = ddf_new_rows_from_api[meta_act_references.columns.tolist()]

# ddf_new_rows_from_api = ddf_new_rows_from_api.map_partitions(
#     lambda df: df.assign(date_of_change_annotation=pd.to_datetime(df['date_of_change_annotation'], format='%Y-%m-%d', errors='coerce')),
#     meta=meta_act_references
# )
ddf_new_rows_from_api['date_of_change_annotation'] = ddf_new_rows_from_api.map_partitions(
    lambda df: pd.to_datetime(df['date_of_change_annotation'], format='%Y-%m-%d', errors='coerce'),
    meta=('date_of_change_annotation', 'datetime64[ns]')
)

kkkfff = ddf_new_rows_from_api.compute()

# new_rows_in_api['date_of_change_annotation_api'] = pd.to_datetime(new_rows_in_api['date_of_change_annotation_api'], format='%Y-%m-%d')

new_rows_in_api_filtered_date = ddf_new_rows_from_api[
    ddf_new_rows_from_api['date_of_change_annotation'] < pd.Timestamp(datetime.now())
    ]

ssssssssss = new_rows_in_api_filtered_date.compute()
### TODO it will be stored in DULegalDocumentsMetaDataChanges_v2


ll = ddf_from_parquet.compute()

new_rows_in_api_filtered_date_grouped_before_checking_date = new_rows_in_api[['ELI']].groupby('ELI').apply(lambda x: x)
new_rows_in_api_filtered_date_grouped_correct = new_rows_in_api_filtered_date[['ELI']].drop_duplicates()

rows_from_api_grouped = df_final_api_ref_computed[['ELI']].drop_duplicates()

# ddf_from_parquet_compute =  ddf_from_parquet.compute()


# df_final_api_ref_computed

merged_parquet_with_api = ddf_from_parquet.merge(
    new_rows_in_api_filtered_date_grouped_correct[['ELI']],
    on='ELI',
    how='left',  # Keep all rows from ddf_from_parquet
    indicator=True
)

jjjj = merged_parquet_with_api.compute()

merged_parquet_with_api = merged_parquet_with_api.rename(columns={'_merge': 'have_new_changes_references'})

merged_parquet_with_api['have_new_changes_references'] = merged_parquet_with_api['have_new_changes_references'].replace(
    {
        'both': 'Yes',
        'left_only': 'No'
    })

jjjj2 = merged_parquet_with_api.compute()

merged_parquet_with_api_with_checking_in_api = merged_parquet_with_api.merge(
    df_final_api_ref[['ELI']].drop_duplicates()[['ELI']],
    on='ELI',
    how='left',  # Keep all rows from ddf_from_parquet
    indicator=True
)

merged_parquet_with_api_with_checking_in_api = merged_parquet_with_api_with_checking_in_api.rename(
    columns={'_merge': 'have_any_references'})

merged_parquet_with_api_with_checking_in_api['have_any_references'] = merged_parquet_with_api_with_checking_in_api[
    'have_any_references'].replace({
    'both': 'Yes',
    'left_only': 'No'
})

merged_parquet_with_api_with_checking_in_api_computed = merged_parquet_with_api_with_checking_in_api.compute()

ddf_rows_legal_acts_to_update = merged_parquet_with_api_with_checking_in_api[
    (merged_parquet_with_api_with_checking_in_api['have_new_changes_references'] == 'Yes') |
    (merged_parquet_with_api_with_checking_in_api['have_any_references'] == 'No')
    ]

fajna_fajna = ddf_rows_legal_acts_to_update.compute()

ddf_rows_legal_acts_to_update = ddf_rows_legal_acts_to_update[meta.columns.tolist()]

# ddf_rows_legal_acts_to_update.map_partitions(insert_partition_to_dynamodb, table_name="DULegalDocumentsMetaData", meta=meta).compute()


ddf_rows_legal_acts_to_update_computed = ddf_rows_legal_acts_to_update.compute()

ll = 4

# dynamodb_ddf = ddf_segments.map_partitions(lambda df: fetch_segment_data(df, kkk, AWS_REGION, 10), meta=meta).compute()

# Use `map_partitions` to scan each segment of the DynamoDB table in parallel


s3_path = f"s3://{DATALAKE_BUCKET}/api-sejm/11111/DU/*"

session = boto3.Session(region_name=AWS_REGION)
credentials = session.get_credentials().get_frozen_credentials()

storage_options = {
    'key': credentials.access_key,
    'secret': credentials.secret_key,
    'client_kwargs': {
        'region_name': AWS_REGION
    }
}

ddf = dd.read_json(s3_path, lines=True, storage_options=storage_options)

ddf = ddf.rename(columns={'year': 'document_year'})
# filtered_ddf = ddf[(ddf['type'].isin(['Ustawa', 'Rozporządzenie'])) & (ddf['status'] == 'akt posiada tekst jednolity')]
filtered_ddf = ddf[(ddf['type'].isin(['Ustawa', 'Rozporządzenie']))]

ddf_api = filtered_ddf.set_index('document_year', drop=False)

merged_ddf = ddf_api.merge(dynamodb_ddf, on='ELI', how='left', suffixes=('_api', '_db'))

ss = merged_ddf.compute()

l = 4


def compare_rows(row, columns_order, columns_to_compare):
    row_result = pd.Series({col.replace('_api', ''): row[col] for col in row.index if col.endswith('_api')})
    row_result['ELI'] = row['ELI']

    # Check if the row exists in DynamoDB (if any key field from the DynamoDB side is NaN, it's missing)
    if pd.isna(row['document_year_db']):  # or you can check another field from DynamoDB
        row_result['missing_in_dynamodb'] = True
        for col in columns_to_compare:
            row_result[f'{col}_changed'] = None
    else:
        # Compare the values from API and DynamoDB
        row_result['missing_in_dynamodb'] = False
        for col in columns_to_compare:
            if row[f'{col}_db'] != row[f'{col}_api']:
                row_result[f'{col}_changed'] = True
            else:
                row_result[f'{col}_changed'] = False

    return row_result.reindex(columns_order)


meta_kk = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'document_year': pd.Series(dtype='int'),
    'status': pd.Series(dtype='str'),
    'announcementDate': pd.Series(dtype='str'),
    'volume': pd.Series(dtype='int'),
    'address': pd.Series(dtype='str'),
    'displayAddress': pd.Series(dtype='str'),
    'promulgation': pd.Series(dtype='str'),
    'pos': pd.Series(dtype='int'),
    'publisher': pd.Series(dtype='str'),
    'changeDate': pd.Series(dtype='str'),
    'textHTML': pd.Series(dtype='bool'),
    'textPDF': pd.Series(dtype='bool'),
    'title': pd.Series(dtype='str'),
    'type': pd.Series(dtype='str'),
    'missing_in_dynamodb': pd.Series(dtype='bool'),
    'changeDate_changed': pd.Series(dtype='bool'),
    'status_changed': pd.Series(dtype='bool'),
    'title_changed': pd.Series(dtype='bool'),
    'promulgation_changed': pd.Series(dtype='bool')
})

rows_which_could_change = ['changeDate', 'status', 'title', 'promulgation']

changes_ddf = merged_ddf.map_partitions(
    lambda df: df.apply(compare_rows, axis=1, args=(list(meta_kk.columns), rows_which_could_change,)), meta=meta_kk)

ddf_to_changed = changes_ddf[
    (changes_ddf['missing_in_dynamodb'] == True) |  # Rows missing in DynamoDB
    (changes_ddf['changeDate_changed'] == True) |  # Changed properties
    (changes_ddf['status_changed'] == True) |
    (changes_ddf['title_changed'] == True) |
    (changes_ddf['promulgation_changed'] == True)
    ]

columns_to_drop = ['missing_in_dynamodb', 'changeDate_changed', 'status_changed', 'title_changed',
                   'promulgation_changed']
ddf_cleaned = ddf_to_changed[
    ~((ddf_to_changed['missing_in_dynamodb'] == True) & (ddf_to_changed['status'] != 'akt posiada tekst jednolity'))]
ddf_cleaned = ddf_cleaned.drop(columns=columns_to_drop, errors='ignore')

s3_path = 's3://datalake-bucket-123/stages/rows_to_update_dynamodb.parquet.gzip'

ddf_cleaned.to_parquet(s3_path, engine='auto', compression='snappy', write_index=False, storage_options=storage_options)

mia_mia = dd.read_parquet(s3_path, engine='auto', storage_options=storage_options)

mia_mia_compute = mia_mia.compute()

print("Dask DataFrame successfully written to S3 in Parquet format")

result = ddf_to_changed.compute()

result2 = ddf_cleaned.compute()

kk = merged_ddf.compute()

ww = filtered_ddf.npartitions

#
# ddf = filtered_ddf.repartition(npartitions=10)
# ll = ddf.npartitions
r = 4


def insert_partition_to_dynamodb(df_partition, table_name):
    boto_session = boto3.Session(region_name=AWS_REGION)
    dynamodb = boto_session.resource('dynamodb')
    documents_metadata_table = dynamodb.Table(table_name)
    # Insert rows into DynamoDB
    with documents_metadata_table.batch_writer() as batch:
        for _, row in df_partition.iterrows():
            try:
                batch.put_item(Item=row.to_dict())
            except Exception as e:
                print(f"Error inserting row: {row.get('id', 'unknown id')}, error: {e}")


ddf_cleaned.map_partitions(insert_partition_to_dynamodb, table_name="DULegalDocumentsMetaData", meta=meta).compute()

l = 4

filtered_ddf.map_partitions(insert_partition_to_dynamodb, meta=pd.DataFrame()).compute()
# with tqdm(total=len(futures), desc="Inserting Partitions", unit="partition") as pbar:
#     for future in as_completed(futures):
#         pbar.update(1)
#         # You can inspect the result or handle exceptions here if needed
#         try:
#             future.result()
#         except Exception as e:
#             print(f"Error with partition: {e}")

r = 4
# Optionally, verify the connection and check the Dask cluster status
print(client)

s = 4


# Submit a test computation to the Dask cluster
def square(x):
    return x ** 2


futures = client.map(square, range(10))
results = client.gather(futures)
print(results)

# Inspect the futures to see which worker executed each task
for future in futures:
    who_has = client.who_has(future)
    print(f"Task {future.key} executed on workers: {who_has}")

# %%
