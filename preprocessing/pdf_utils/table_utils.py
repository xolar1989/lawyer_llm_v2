from typing import List, Union, Tuple

import pdfplumber
from pdfplumber.table import Table

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs


class TableCoordinates:
    def __init__(self, start_x: float, end_x: float, start_y: float, end_y: float):
        self.start_x = start_x
        self.end_x = end_x
        self.start_y = start_y
        self.end_y = end_y

    @property
    def bbox(self):
        return self.start_x, self.start_y, self.end_x, self.end_y


class TableDetector:

    @staticmethod
    def _get_sorted_tables(tables_in_page: List[Table], paragraph: LegalActPageRegionParagraphs,
                           page: pdfplumber.pdf.Page) -> List[Table]:
        sorted_tables = []

        page_paragraph = page.within_bbox(paragraph.bbox)
        page_text = page_paragraph.extract_text().strip()
        for table_index, table in enumerate(tables_in_page):
            bbox_table_corrected = (
                paragraph.start_x,
                table.bbox[1],
                paragraph.end_x,
                table.bbox[3]
            )
            if not TableDetector.is_table_bbox_within_paragraph(bbox_table_corrected, paragraph.bbox):
                aws_logger.error(f"Invalid state: table of bounds")

                continue

            text_of_table = page_paragraph.within_bbox(bbox_table_corrected).extract_text().strip()

            if text_of_table not in page_text:
                aws_logger.error(f"Invalid state: text of table not found in page text, text_of_table: "
                                 f"{text_of_table}, page_text: {page_text}, page_number: {page_paragraph.page_number}")
                continue

            # Find the start index of the table in the page text
            table_start_index = page_text.index(text_of_table)
            sorted_tables.append((table_start_index, table))

        # Sort tables by their start index
        sorted_tables.sort(key=lambda x: x[0])

        # Return only the sorted tables
        return [table for _, table in sorted_tables]

    @classmethod
    def _have_overlapping_region(cls, evaluated_table: Table,
                                 evaluated_table_page_regions: List[TableCoordinates]
                                 ) \
            -> bool:

        table_bbox_corrected = (evaluated_table.page.bbox[0],
                                evaluated_table.bbox[1],
                                evaluated_table.page.bbox[2],
                                evaluated_table.bbox[3])
        for i, page_region in enumerate(evaluated_table_page_regions):
            if cls._is_bbox_region_is_overlapping(
                    table_bbox_corrected,
                    page_region.bbox
            ):
                return True

        return False

    @classmethod
    def _get_overlapping_region(cls, evaluated_table: Table,
                                evaluated_table_page_regions: List[TableCoordinates]) \
            -> Union[Tuple[TableCoordinates, int], None]:
        table_bbox_corrected = (evaluated_table.page.bbox[0],
                                evaluated_table.bbox[1],
                                evaluated_table.page.bbox[2],
                                evaluated_table.bbox[3])
        for i, page_region in enumerate(evaluated_table_page_regions):
            if cls._is_bbox_region_is_overlapping(
                    table_bbox_corrected,
                    page_region.bbox
            ):
                return page_region, i

        return None

    @staticmethod
    def is_table_bbox_within_paragraph(table_bbox, paragraph_bbox):
        return (
                table_bbox[0] >= paragraph_bbox[0] and
                table_bbox[1] >= paragraph_bbox[1] and
                table_bbox[2] <= paragraph_bbox[2] and
                table_bbox[3] <= paragraph_bbox[3]
        )

    @staticmethod
    def _is_bbox_region_is_overlapping(bbox_to_add, bbox_added):
        """Check if two bounding boxes overlap."""
        x1_min, y1_min, x1_max, y1_max = bbox_to_add
        x2_min, y2_min, x2_max, y2_max = bbox_added

        # Check for overlap
        return not (x1_max <= x2_min or x2_max <= x1_min or y1_max <= y2_min or y2_max <= y1_min)

    @staticmethod
    def _is_index_overlapping(index_range1, index_range2):
        """Check if two index ranges overlap."""
        start1, end1 = index_range1
        start2, end2 = index_range2

        # Check for overlap
        return not (end1 <= start2 or end2 <= start1)

    @classmethod
    def _merge_overlapping_tables(cls, tables_on_page: List[Table]) \
            -> List[TableCoordinates]:
        merged_tables: List[TableCoordinates] = []
        for table in tables_on_page:
            if not cls._have_overlapping_region(table, merged_tables):
                merged_tables.append(TableCoordinates(
                    start_x=table.bbox[0],
                    end_x=table.bbox[2],
                    start_y=table.bbox[1],
                    end_y=table.bbox[3]
                )
                )
            else:
                overlapping_table, merged_index = cls._get_overlapping_region(table, merged_tables)
                merged_tables[merged_index] = TableCoordinates(
                    start_x=min(table.bbox[0], overlapping_table.start_x),
                    end_x=max(table.bbox[2], overlapping_table.end_x),
                    start_y=min(table.bbox[1], overlapping_table.start_y),
                    end_y=max(table.bbox[3], overlapping_table.end_y)
                )

        return merged_tables

    @classmethod
    def extract(cls, page: pdfplumber.pdf.Page, paragraph: LegalActPageRegionParagraphs) -> List[TableCoordinates]:
        tables_in_page = page.find_tables()
        tables_in_page = cls._get_sorted_tables(tables_in_page, paragraph, page)
        return cls._merge_overlapping_tables(tables_in_page)
