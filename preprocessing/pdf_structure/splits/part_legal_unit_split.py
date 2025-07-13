import re
from typing import List

from preprocessing.pdf_structure.splits.chapter_split import ChapterSplit
from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splits.unit_split import LegalUnitSplit


class PartLegalUnitSplit(LegalUnitSplit):

    def __init__(self, split: TextSplit, is_hidden: bool = False):
        super().__init__(split)
        self.is_hidden = is_hidden
        self.chapters: List[ChapterSplit] = []

    @property
    def id_unit(self):
        if self.is_hidden:
            return 'I'
        match_of_identification = re.match(self._can_erase_number_pattern(), self.split.text)
        identification_start = self.split.start_index + match_of_identification.start(1)
        identification_end = self.split.start_index + match_of_identification.end(1) - 1

        return self.split.build_text_split_from_indexes(identification_start, identification_end).text

    @property
    def title(self):
        if self.is_hidden:
            return ""
        title_search = re.search(r'(DZIAŁ\s+[IVXLCDMA-Z]+)\s*\s*((?:.+\n)*?)^\s*(?=Rozdział|Art\.)', self.split.text,
                                 flags=re.MULTILINE)

        title = title_search.group(2)
        title_index_start = self.split.start_index + title_search.start(2)
        title_index_end = self.split.start_index + title_search.end(2) - 1

        return self.split.build_text_split_from_indexes(title_index_start, title_index_end).text

    @classmethod
    def _can_erase_number_pattern(cls):
        return r'^DZIAŁ\s+([IVXLCDMA-Z]+)'

    def split_item_for_further_processing(self):
        if self.is_hidden:
            return self.split

        outside_search = re.search(r'(DZIAŁ\s+[IVXLCDMA-Z]+)\s*\s*((?:.+\n)*?)^\s*(?=Rozdział|Art\.)', self.split.text,
                                   flags=re.MULTILINE)

        outside_index_start = self.split.start_index + outside_search.start(0)
        outside_index_end = self.split.start_index + outside_search.end(0) - 1

        return self.split.build_text_split_from_indexes(outside_index_end + 1, self.split.end_index)