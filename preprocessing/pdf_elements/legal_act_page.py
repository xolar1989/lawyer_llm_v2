from typing import List, Mapping, Any, Dict

import pdfplumber
import re

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.pdf_boundaries.boundaries import BoundariesArea
from preprocessing.pdf_boundaries.footer_boundaries import LegalActFooterArea
from preprocessing.pdf_boundaries.margin_boundaries import LegalActMarginArea
from preprocessing.pdf_boundaries.paragraph_boundaries import LegalActParagraphArea, ParagraphAreaType
from preprocessing.pdf_elements.lines import OrderedObjectLegalAct, TableRegionPageLegalAct, TextLinePageLegalAct
from preprocessing.pdf_utils.area_text_extractor import PageAreaTextExtractor
from preprocessing.pdf_utils.multiline_extractor import MultiLineTextExtractor


class LegalActPage(MongodbObject):
    LINE_DELIMITER = ' \n'

    ## TODO check this quit well example D20220974Lj
    def __init__(self, page_number: int, paragraph_lines: List[OrderedObjectLegalAct],
                 footer_lines: List[OrderedObjectLegalAct], margin_lines: List[OrderedObjectLegalAct]):
        self.page_number = page_number
        self.paragraph_lines = paragraph_lines
        self.footer_lines = footer_lines
        self.margin_lines = margin_lines

    @property
    def paragraph_text(self):
        return self.get_text_of_area(self.paragraph_lines)

    @property
    def footer_text(self):
        return self.get_text_of_area(self.footer_lines)

    @property
    def margin_text(self):
        return self.get_text_of_area(self.margin_lines)

    @staticmethod
    def get_text_of_area(lines: List[OrderedObjectLegalAct]):
        return ''.join(p.text + OrderedObjectLegalAct.LINE_DELIMITER for p in lines)

    @property
    def len_paragraph_lines(self):
        return self.get_len_of_lines(self.paragraph_lines)

    @property
    def len_footer_lines(self):
        return self.get_len_of_lines(self.footer_lines)

    @property
    def len_margin_lines(self):
        return self.get_len_of_lines(self.margin_lines)

    @property
    def get_table_lines(self) -> List[TableRegionPageLegalAct]:
        return [line for line in self.paragraph_lines if isinstance(line, TableRegionPageLegalAct)]

    @staticmethod
    def get_len_of_lines(lines: List[OrderedObjectLegalAct]):
        return sum(line.get_len_of_line for line in lines)

    def to_dict(self):
        return {
            "page_number": self.page_number,
            "paragraph_lines": [line.to_dict() for line in self.paragraph_lines],
            "footer_lines": [line.to_dict() for line in self.footer_lines],
            "margin_lines": [line.to_dict() for line in self.margin_lines]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            page_number=dict_object["page_number"],
            paragraph_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["paragraph_lines"]],
            footer_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["footer_lines"]],
            margin_lines=[OrderedObjectLegalAct.from_dict(line) for line in dict_object["margin_lines"]]
        )

    @classmethod
    def fallback_get_lines_of_specific_area(cls, text_extractor: PageAreaTextExtractor, area: BoundariesArea,
                                            document_id: str, invoke_id: str,
                                            page: pdfplumber.pdf.Page, current_index_of_page: int) \
            -> List[TextLinePageLegalAct]:

        text_lines = page.within_bbox(area.bbox).extract_text_lines()
        text_from_llm = text_extractor.parse(area, page, text_lines, document_id, invoke_id)
        text_from_llm_lines = PageAreaTextExtractor.get_lines(text_from_llm)
        line_obj_list = []
        current_index = current_index_of_page
        for line_index, line_dict in enumerate(text_lines):
            line_obj = TextLinePageLegalAct.build_single_line_from_fallback(line_dict, page,
                                                                            text_from_llm_lines[line_index],
                                                                            current_index)
            current_index += line_obj.get_len_of_line
            line_obj_list.append(line_obj)
        return line_obj_list

    @classmethod
    def can_be_decoded_by_ocr(cls, area: BoundariesArea, page: pdfplumber.pdf.Page):
        text_of_area = page.within_bbox(area.bbox).extract_text()

        return len(re.findall(r'\(cid:\d+\)', text_of_area)) == 0

    @classmethod
    def merge_dict_text_line(cls, current_text_dict_line: Dict[str, Any],
                             prev_text_dict_line: Dict[str, Any],
                             page: pdfplumber.pdf.Page) -> Dict[str, Any]:
        current_number = max([char['num_line'] for char in prev_text_dict_line['chars']])
        current_text_dict_line['chars'] = [{**char, 'num_line': current_number + 1} for char in
                                           current_text_dict_line['chars']]
        prev_text_dict_line['chars'].extend(current_text_dict_line['chars'])
        prev_text_dict_line['chars'] = sorted(prev_text_dict_line['chars'], key=lambda c: c['x0'])
        prev_text_dict_line['top'] = min(prev_text_dict_line['top'], current_text_dict_line['top'])
        prev_text_dict_line['bottom'] = max(prev_text_dict_line['bottom'], current_text_dict_line['bottom'])
        prev_text_dict_line['x0'] = min(prev_text_dict_line['x0'], current_text_dict_line['x0'])
        prev_text_dict_line['x1'] = max(prev_text_dict_line['x1'], current_text_dict_line['x1'])
        bbox = (
            prev_text_dict_line['x0'], prev_text_dict_line['top'], prev_text_dict_line['x1'],
            prev_text_dict_line['bottom'])
        prev_text_dict_line["text"] = page.within_bbox(bbox).extract_text()
        prev_text_dict_line['merged'] = True
        return prev_text_dict_line

    @classmethod
    def create_lines_of_specific_area(cls, document_id: str,
                                      invoke_id: str,
                                      area: BoundariesArea,
                                      current_index_of_page: int,
                                      page: pdfplumber.pdf.Page,
                                      multiline_llm_extractor: MultiLineTextExtractor,
                                      threshold: float = 0.4) \
            -> List[TextLinePageLegalAct]:
        text_lines_paragraph_region = page.within_bbox(area.bbox).extract_text_lines()
        text_lines = [
            {
                **line,
                'merged': False,
                'chars': [{**char, 'num_line': 1} for char in line['chars']]
            }
            for line in sorted(text_lines_paragraph_region, key=lambda x: x['top'])
        ]
        # if len(text_lines) == 0:
        #     return []

        height_counts = area.get_heights_of_chars(page)
        most_common_height, _ = height_counts.most_common(1)[0]

        lines_dict_list = []
        line_obj_list = []
        current_index = current_index_of_page

        for current_text_dict_line in text_lines:
            if not lines_dict_list:
                lines_dict_list.append(current_text_dict_line)

            elif current_text_dict_line['top'] + threshold <= lines_dict_list[-1]['bottom'] and current_text_dict_line[
                'bottom'] >= lines_dict_list[-1]['top']:
                lines_dict_list[-1] = cls.merge_dict_text_line(
                    current_text_dict_line=current_text_dict_line,
                    prev_text_dict_line=lines_dict_list[-1],
                    page=page
                )
            else:
                if lines_dict_list[-1]['merged']:
                    line_obj = TextLinePageLegalAct.build_using_multi_line(
                        document_id=document_id,
                        invoke_id=invoke_id,
                        line_dict=lines_dict_list[-1],
                        page=page,
                        current_index=current_index,
                        current_line_on_page=len(line_obj_list),
                        multiline_llm_extractor=multiline_llm_extractor
                    )
                else:
                    line_obj = TextLinePageLegalAct.build_using_single_line(
                        line_dict=lines_dict_list[-1],
                        current_index=current_index,
                        page=page,
                        common_height_char=most_common_height
                    )
                w = line_obj.text
                current_index += line_obj.get_len_of_line
                line_obj_list.append(line_obj)
                lines_dict_list.append(current_text_dict_line)
        if lines_dict_list:
            if lines_dict_list[-1]['merged']:
                line_obj = TextLinePageLegalAct.build_using_multi_line(
                    document_id=document_id,
                    invoke_id=invoke_id,
                    line_dict=lines_dict_list[-1],
                    page=page,
                    current_index=current_index,
                    current_line_on_page=len(line_obj_list),
                    multiline_llm_extractor=multiline_llm_extractor
                )
            else:
                line_obj = TextLinePageLegalAct.build_using_single_line(
                    line_dict=lines_dict_list[-1],
                    current_index=current_index,
                    page=page,
                    common_height_char=most_common_height
                )
            current_index += line_obj.get_len_of_line
            line_obj_list.append(line_obj)

        return line_obj_list

    @classmethod
    def build(cls, page: pdfplumber.pdf.Page,
              document_id: str,
              invoke_id: str,
              paragraph_areas: List[LegalActParagraphArea],
              margin_areas: List[LegalActMarginArea],
              footer_areas: List[LegalActFooterArea],
              current_index_of_legal_act: int, current_table_number: int,
              current_index_of_footer_legal_act: int,
              current_index_of_margin_legal_act: int,
              multiline_llm_extractor: MultiLineTextExtractor,
              fallback_text_extractor: PageAreaTextExtractor) -> 'LegalActPage':
        paragraph_lines: List[OrderedObjectLegalAct] = []
        footer_lines: List[OrderedObjectLegalAct] = []
        margin_lines: List[OrderedObjectLegalAct] = []
        current_index_of_page = current_index_of_legal_act
        current_table_number_on_page = current_table_number
        current_index_footer = current_index_of_footer_legal_act
        for footer_area in footer_areas:
            if not cls.can_be_decoded_by_ocr(footer_area, page):
                print(
                    f"Fallback for reading area is calling, document_id: {document_id}, page:{page.page_number}, area: footer")

            footer_lines = cls.create_lines_of_specific_area(
                document_id=document_id,
                invoke_id=invoke_id,
                area=footer_area,
                current_index_of_page=current_index_footer,
                page=page,
                multiline_llm_extractor=multiline_llm_extractor,
                threshold=0.7
            ) if cls.can_be_decoded_by_ocr(footer_area, page) else \
                cls.fallback_get_lines_of_specific_area(
                    text_extractor=fallback_text_extractor,
                    area=footer_area,
                    page=page,
                    current_index_of_page=current_index_of_page,
                    document_id=document_id,
                    invoke_id=invoke_id,
                )
            current_index_footer += cls.get_len_of_lines(footer_lines)

        current_index_of_margin = current_index_of_margin_legal_act
        for margin_area in margin_areas:
            if not cls.can_be_decoded_by_ocr(margin_area, page):
                print(
                    f"Fallback for reading area is calling, document_id: {document_id}, page:{page.page_number}, area: margin")

            margin_lines_region = cls.create_lines_of_specific_area(
                document_id=document_id,
                invoke_id=invoke_id,
                area=margin_area,
                current_index_of_page=current_index_of_margin,
                page=page,
                multiline_llm_extractor=multiline_llm_extractor
            ) if cls.can_be_decoded_by_ocr(margin_area, page) else \
                cls.fallback_get_lines_of_specific_area(
                    text_extractor=fallback_text_extractor,
                    area=margin_area,
                    page=page,
                    current_index_of_page=current_index_of_page,
                    document_id=document_id,
                    invoke_id=invoke_id,
                )
            current_index_of_margin += cls.get_len_of_lines(margin_lines_region)
            margin_lines.extend(margin_lines_region)

        for paragraph_area in paragraph_areas:
            if paragraph_area.area_type == ParagraphAreaType.TABLE:
                table_region = TableRegionPageLegalAct(
                    start_x=paragraph_area.start_x,
                    end_x=paragraph_area.end_x,
                    start_y=paragraph_area.start_y,
                    end_y=paragraph_area.end_y,
                    page_number=page.page_number,
                    start_index_in_act=current_index_of_page,
                    table_number=current_table_number_on_page
                )
                current_table_number_on_page += 1
                current_index_of_page += len(table_region) + len(cls.LINE_DELIMITER)
                paragraph_lines.append(table_region)
            else:
                if not cls.can_be_decoded_by_ocr(paragraph_area, page):
                    print(
                        f"Fallback for reading area is calling, document_id: {document_id}, page:{page.page_number}, area: paragraph")

                paragraph_text_lines_region = cls.create_lines_of_specific_area(
                    document_id=document_id,
                    invoke_id=invoke_id,
                    area=paragraph_area,
                    current_index_of_page=current_index_of_page,
                    page=page,
                    multiline_llm_extractor=multiline_llm_extractor
                ) if cls.can_be_decoded_by_ocr(paragraph_area, page) else \
                    cls.fallback_get_lines_of_specific_area(
                        text_extractor=fallback_text_extractor,
                        area=paragraph_area,
                        page=page,
                        current_index_of_page=current_index_of_page,
                        document_id=document_id,
                        invoke_id=invoke_id,
                    )

                current_index_of_page += cls.get_len_of_lines(paragraph_text_lines_region)
                paragraph_lines.extend(paragraph_text_lines_region)

        return cls(
            page_number=page.page_number,
            paragraph_lines=paragraph_lines,
            footer_lines=footer_lines,
            margin_lines=margin_lines
        )
