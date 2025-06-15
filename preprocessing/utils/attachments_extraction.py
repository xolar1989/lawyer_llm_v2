import re
from typing import List, Union, Tuple, Set, Dict, Any, Match, Mapping

import pdfplumber

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.utils.page_regions import LegalActPageRegion, LegalActPageRegionParagraphs, \
    LegalActPageRegionMarginNotes, LegalActPageRegionFooterNotes


class PageRegions(MongodbObject):

    def __init__(self, paragraphs: List['LegalActPageRegionParagraphs'],
                 margin_notes: List['LegalActPageRegionMarginNotes'],
                 footer_notes: List['LegalActPageRegionFooterNotes'],
                 attachments: List['LegalActPageRegionAttachment']
                 ):
        self.paragraphs = paragraphs
        self.margin_notes = margin_notes
        self.footer_notes = footer_notes
        self.attachments = attachments


    def to_dict(self):
        return {
            "paragraphs": [paragraph.to_dict() for paragraph in self.paragraphs],
            "margin_notes": [margin_note.to_dict() for margin_note in self.margin_notes],
            "footer_notes": [footer_note.to_dict() for footer_note in self.footer_notes],
            "attachments": [attachment.to_dict() for attachment in self.attachments]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            paragraphs=[LegalActPageRegionParagraphs.from_dict(paragraph) for paragraph in dict_object["paragraphs"]],
            margin_notes=[LegalActPageRegionMarginNotes.from_dict(margin_note) for margin_note in dict_object["margin_notes"]],
            footer_notes=[LegalActPageRegionFooterNotes.from_dict(footer_note) for footer_note in dict_object["footer_notes"]],
            attachments=[AttachmentRegion.from_dict(attachment) for attachment in dict_object["attachments"]]
        )

class AttachmentPageRegion(MongodbObject):


    def __init__(self, start_x: float, start_y: float, end_x: float, end_y: float, page_number: int, page_height: float):
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

    def to_dict(self):
        return {
            "start_x": self.start_x,
            "start_y": self.start_y,
            "end_x": self.end_x,
            "end_y": self.end_y,
            "page_number": self.page_number,
            "page_height": self.page_height
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            start_x=dict_object["start_x"],
            start_y=dict_object["start_y"],
            end_x=dict_object["end_x"],
            end_y=dict_object["end_y"],
            page_number=dict_object["page_number"],
            page_height=dict_object["page_height"]
        )


class AttachmentRegion(MongodbObject):

    def __init__(self, page_regions: List[AttachmentPageRegion], name: str):
        self._page_regions = page_regions
        self._name = name

    def to_dict(self):
        return {
            "page_regions": [page_region.to_dict() for page_region in self.page_regions],
            "name": self.name
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            page_regions=[AttachmentPageRegion.from_dict(page_region) for page_region in dict_object["page_regions"]],
            name=dict_object["name"]
        )

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
                if match_text in candidate_text and AttachmentRegion.is_start_of_the_page(text_before_start):

                    return attachment_page_region.start_y, attachment_page_region.page_number
                elif match_text in candidate_text:
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
            if next_attachment_header == 'Załącznik nr 5':
                r = 4
                text = page.within_bbox(page_attachment_bbox).extract_text()
            match_text = re.sub(r'[\s\n\r]+', ' ', next_attachment_header)
            for idx, word in enumerate(words):
                # Join words to compare with match_text, and check within a reasonable range
                text_before_start = ' '.join(w['text'] for w in words[:idx])
                candidate_text = ' '.join(w['text'] for w in words[idx:idx + len(match_text.split())])
                if match_text in candidate_text and AttachmentRegion.is_start_of_the_page(text_before_start):
                    if index < 1:
                        raise ValueError(
                            f"Invalid state ,Attachment header {next_attachment_header} not found in the attachments page regions")
                    return attachments_page_regions[index - 1].end_y, attachments_page_regions[index - 1].page_number
                elif match_text in candidate_text:
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


def get_matches_for_attachments(text:str) -> List[Match[bytes]]:
    """
    Find matches for attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of matches for attachments
    """
    patterns = [
        LegalActPageRegion.LEGAL_ANNOTATION_PATTERN_1,
        LegalActPageRegion.LEGAL_ANNOTATION_PATTERN_2,
    ]

    for pattern in patterns:
        matches = list(re.finditer(pattern, text, re.DOTALL))
        if matches:
            return matches

    return []

def get_pages_with_attachments(pdf: pdfplumber.pdf.PDF) -> Set[int]:
    """
    Find pages with attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of page numbers with attachments
    """
    attachment_pages = set()
    for page_index in range(len(pdf.pages)):
        try:
            page = pdf.pages[page_index]
            text = page.extract_text()
            matches = get_matches_for_attachments(text)
            if any(matches):
                for page_from_index in range(page_index, len(pdf.pages)):
                    page_iter = pdf.pages[page_from_index]
                    attachment_pages.add(page_iter.page_number)
                    del page_iter
                break
            del text
        finally:
            pdf.flush_cache()
            del page

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
        matches = get_matches_for_attachments(text_of_page_within_attachment)
        for match in matches:
            header = match.group(1).strip()
            header = re.sub(r'(\d)\)$', "", header) ## TODO TO fix it ?? added due to this case Załącznik nr 63)
            if header in attachment_headers:
                print(f"Invalid state in finding attachments headers: {header} already found, skipping... ")
            else:
                attachment_headers.append(header)
        del page
    return attachment_headers
