import re
from typing import List

from preprocessing.pdf_structure.splits.chapter_split import ChapterSplit
from preprocessing.pdf_structure.splits.part_legal_unit_split import PartLegalUnitSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class ChapterSplitter(AbstractDocumentSplitter):

    def before_upcoming_change_pattern(self):
        pass

    def upcoming_change_pattern(self):
        pass

    def split_function(self, text):
        return re.finditer(
            r'R\s*o\s*z\s*d\s*z\s*i\s*a\s*[łl]\s+[1-9a-zA-Z]+[\s\S]*?(?=R\s*o\s*z\s*d\s*z\s*i\s*a\s*[łl]\s+[1-9a-zA-Z]+|\Z)',
            text)

    def split(self, part_unit_split: PartLegalUnitSplit):
        chapter_splits: List[ChapterSplit] = []
        text_split = part_unit_split.split_item_for_further_processing()
        for part_match in self.split_function(text_split.text):
            start_index_match = text_split.start_index + part_match.start()
            end_index_match = text_split.start_index + part_match.end() - 1

            chapter_split = ChapterSplit(text_split.build_text_split_from_indexes(
                left_index=start_index_match,
                right_index=end_index_match
            )
            )

            chapter_splits.append(chapter_split)
        if len(chapter_splits) == 0:
            chapter_split = ChapterSplit(part_unit_split.split, is_hidden=True)
            chapter_splits.append(chapter_split)
        part_unit_split.chapters = chapter_splits
        return chapter_splits