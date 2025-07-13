import re
from abc import ABC, abstractmethod

from preprocessing.pdf_structure.splits.text_split import TextSplit, StatusOfText


class LegalUnitSplit(ABC):
    def __init__(self, split: TextSplit):
        self.split = split

    @property
    def is_current_unit(self):
        return self.split.action_type == StatusOfText.BEFORE_UPCOMING_CHANGE or \
            self.split.action_type == StatusOfText.NOT_MATCH_CHANGE

    @property
    def id_unit(self):
        match_of_identification = re.match(self._can_erase_number_pattern(), self.split.text)

        identification_start = self.split.start_index + match_of_identification.start(1)
        identification_end = self.split.start_index + match_of_identification.end(1) - 1

        return self.split.build_text_split_from_indexes(identification_start, identification_end).text

    @classmethod
    @abstractmethod
    def _can_erase_number_pattern(cls):
        pass

    @abstractmethod
    def split_item_for_further_processing(self):
        pass