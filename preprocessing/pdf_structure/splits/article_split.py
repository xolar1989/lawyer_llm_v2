import re
from dataclasses import dataclass
from typing import List

from preprocessing.pdf_structure.splits.text_split import TextSplit
from preprocessing.pdf_structure.splits.unit_split import LegalUnitSplit


@dataclass
class ArticleSplit(LegalUnitSplit):

    def __init__(self, art_split: TextSplit):
        super().__init__(art_split)
        self.legal_units_indeed: List[LegalUnitSplit] = []

    def set_legal_indeed_units(self, legal_units_indeed: List[LegalUnitSplit]):
        self.legal_units_indeed = legal_units_indeed

    def split_item_for_further_processing(self):
        match_of_identification = re.match(self._can_erase_number_pattern(), self.split.text)

        to_erase_start = self.split.start_index + match_of_identification.start(0)
        to_erase_end = self.split.start_index + match_of_identification.end(0) - 1

        end_index = self.legal_units_indeed[0].split.start_index - 1 if len(self.legal_units_indeed) > 0 else \
            self.split.end_index

        if end_index < to_erase_end + 1:
            return None

        return self.split.build_text_split_from_indexes(to_erase_end + 1, end_index)

    @classmethod
    def _can_erase_number_pattern(cls):
        return r'Art[\.]*\s*(\d+[⁰¹²³⁴⁵⁶⁷⁸⁹ᵃᵇᶜᵈᵉᶠᶢʰⁱʲᵏˡᵐⁿᵒᵖʳᑫˢᵗᵘᵛʷˣʸᶻᵘⁿᵒʳᵐᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᵠᴿˢᵀᵁⱽᵂˣʸᶻᴸa-zA-Z]*)\s*(?:[–-]\s*(?:Art\.\s*)?\d+[⁰¹²³⁴⁵⁶⁷⁸⁹ᵃᵇᶜᵈᵉᶠᶢʰⁱʲᵏˡᵐⁿᵒᵖʳᑫˢᵗᵘᵛʷˣʸᶻᵘⁿᵒʳᵐᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᵠᴿˢᵀᵁⱽᵂˣʸᶻᴸa-zA-Z]*)?\.\s*[⁰¹²³⁴⁵⁶⁷⁸⁹⁾]*\s*'