import re
from typing import List

from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter, T


class StatuteBodySplitter(AbstractDocumentSplitter):

    def filter_splits(self, splits: List[T]) -> List[T]:
        pass

    def before_upcoming_change_pattern(self):
        pass

    def upcoming_change_pattern(self):
        pass

    def split_function(self, text):
        return re.compile(r'(U\s*S\s*T\s*A\s*W\s*A[\s\S]*)', flags=re.DOTALL | re.MULTILINE).search(text)

    def split(self, prev_split: TextSplit):
        statute_body_match = self.split_function(prev_split.text)
        if statute_body_match:
            start_index_match = prev_split.start_index + statute_body_match.start()
            end_index_match = prev_split.start_index + statute_body_match.end() - 1

            statute_body_split = prev_split.build_text_split_from_indexes(
                left_index=start_index_match,
                right_index=end_index_match
            )
            return statute_body_split
        raise RuntimeError("dddd")