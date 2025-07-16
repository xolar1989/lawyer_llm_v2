
import re
from dataclasses import dataclass

from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splits.unit_split import LegalUnitSplit


@dataclass
class PointSplit(LegalUnitSplit):

    def __init__(self, split: TextSplit):
        super().__init__(split)

    @classmethod
    def _can_erase_number_pattern(cls):
        return r'^(?:[<[]?)\s*(\d+[a-z]*)\)\s*(?=[a-ząćęłńóśźżA-ZĄĆĘŁŃÓŚŹŻ0-9⁰¹²³⁴⁵⁶⁷⁸⁹⁾§]|\[|\(|„|ᴮˡᵃᵈ  ᴺⁱᵉ ᶻᵈᵉᶠⁱⁿⁱᵒʷᵃⁿᵒ ᶻᵃᵏˡᵃᵈᵏⁱ·⁾)'

    def split_item_for_further_processing(self) -> TextSplit | None:
        match_of_identification = re.match(self._can_erase_number_pattern(), self.split.text)
        if match_of_identification is None:
            w = 4

        to_erase_start = self.split.start_index + match_of_identification.start(0)
        to_erase_end = self.split.start_index + match_of_identification.end(0) - 1

        return self.split.build_text_split_from_indexes(to_erase_end + 1, self.split.end_index)