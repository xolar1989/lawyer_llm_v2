import re
from typing import List

from preprocessing.pdf_structure.splits.part_legal_unit_split import PartLegalUnitSplit
from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splits.title_unit_split import TitleUnitSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class TitleUnitSplitter(AbstractDocumentSplitter):
    def before_upcoming_change_pattern(self):
        pass

    def upcoming_change_pattern(self):
        pass

    def split_function(self, text):
        return re.finditer(r'(^T\s*Y\s*T\s*U\s*Ł\s+[IVXLCDM]+[\s\S]*?)(?=^T\s*Y\s*T\s*U\s*Ł\s+[IVXLCDM]+|\Z)', text, flags=re.DOTALL | re.MULTILINE)

    def split(self, prev_split: TextSplit):
        title_units_splits: List[TitleUnitSplit] = []
        for part_match in self.split_function(prev_split.text):
            text_of_part = part_match.group()
            start_index_match = prev_split.start_index + part_match.start()
            end_index_match = prev_split.start_index + part_match.end() - 1

            title_unit_split = prev_split.build_text_split_from_indexes(
                left_index=start_index_match,
                right_index=end_index_match
            )

            unit_split = TitleUnitSplit(title_unit_split)
            title_units_splits.append(unit_split)
        filtered_splits = self.filter_splits(title_units_splits)
        if len(filtered_splits) == 0:
            unit_split = TitleUnitSplit(prev_split, is_hidden=True)
            filtered_splits.append(unit_split)
        return filtered_splits

    def filter_splits(self, title_unit_splits: List[TitleUnitSplit]) -> List[TitleUnitSplit]:
        ## TODO it filter units of final, temporary rules, it could be including in future adjust, now we gonna filter it
        filtered_splits = []
        for title_unit_split in title_unit_splits:
            unit_title = title_unit_split.title.replace("\n", "")
            if not ('przepisy zmieniajace' in self.normalize(unit_title) or
                    'zmiany w przepisach' in self.normalize(unit_title) or
                    'przejsciowe' in self.normalize(unit_title) or
                    'koncowe' in self.normalize(unit_title) or
                    'dostosowujace' in self.normalize(unit_title))\
                    and title_unit_split.split_item_for_further_processing().is_up_to_date():
                filtered_splits.append(title_unit_split)

        return filtered_splits