import re
from typing import List

from preprocessing.pdf_structure.splits.part_legal_unit_split import PartLegalUnitSplit
from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class PartLegalUnitSplitter(AbstractDocumentSplitter):

    def before_upcoming_change_pattern(self):
        pass

    def upcoming_change_pattern(self):
        pass

    def split_function(self, text):
        # return [SplitMatch(match.start(),match.end(), match.group())  for match in re.finditer(r'DZIAŁ\s+[IVXLCDM]+[\s\S]*?(?=DZIAŁ\s+[IVXLCDM]+|\Z)', text)]


        return re.finditer(r'DZIAŁ\s+[IVXLCDM]+[\s\S]*?(?=DZIAŁ\s+[IVXLCDM]+|\Z)', text)

    def split(self, prev_split: TextSplit):

        rr = list(self.split_function(prev_split.text))

        part_units_splits: List[PartLegalUnitSplit] = []
        for part_match in self.split_function(prev_split.text):
            text_of_part = part_match.group()
            start_index_match = prev_split.start_index + part_match.start()
            end_index_match = prev_split.start_index + part_match.end() - 1

            part_unit_split = prev_split.build_text_split_from_indexes(
                left_index=start_index_match,
                right_index=end_index_match
            )

            unit_split = PartLegalUnitSplit(part_unit_split)
            part_units_splits.append(unit_split)

        if len(part_units_splits) == 0:
            unit_split = PartLegalUnitSplit(prev_split, is_hidden=True)
            part_units_splits.append(unit_split)

        return part_units_splits
    