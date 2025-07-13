import re
from enum import Enum
from typing import Mapping, Any, List

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.pdf_elements.chars import CharLegalAct


class StatusOfText(Enum):
    UPCOMING_CHANGE = "UPCOMING_CHANGE"
    BEFORE_UPCOMING_CHANGE = "BEFORE_UPCOMING_CHANGE"
    NOT_MATCH_CHANGE = "NON_MATCH"


class CharTextSplit(MongodbObject):

    def to_dict(self):
        return {
            'char': self.char.__dict__,
            "action_type": self.action_type.name
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return CharTextSplit(
            char=CharLegalAct(**dict_object['char']),
            action_type=StatusOfText[dict_object["action_type"]]
        )

    def __init__(self, char: CharLegalAct, action_type: StatusOfText):
        self.char = char
        self.action_type = action_type


class TextSplit(MongodbObject):

    def to_dict(self):
        return {

            "chars": [char.to_dict() for char in self.chars],
            "action_type": self.action_type.name
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return TextSplit(
            chars=[CharTextSplit.from_dict(char) for char in dict_object['chars']],
            action_type=StatusOfText[dict_object["action_type"]]
        )

    def __init__(self, chars: List[CharTextSplit], action_type: StatusOfText):
        # self.start_index = start_index
        # self.end_index = end_index
        # self.text = text
        self.chars = sorted(chars, key=lambda split_char: split_char.char.index_in_legal_act)
        self.action_type = action_type

    @property
    def start_index(self):
        return self.chars[0].char.index_in_legal_act

    @property
    def end_index(self):
        return self.chars[len(self.chars) - 1].char.index_in_legal_act

    @property
    def text(self):
        return "".join(legal_char.char.text for legal_char in self.chars)

    def build_text_split_from_indexes(self, left_index: int, right_index: int) -> 'TextSplit':
        chars: List[CharTextSplit] = []
        index_of_first_char_in_tab = self.get_char_index(left_index)
        length_of_sequence = right_index - left_index + 1
        for current_index in range(index_of_first_char_in_tab, length_of_sequence + index_of_first_char_in_tab):
            chars.append(self.chars[current_index])
        return TextSplit(chars, self.action_type)

    def get_chars_from_indexes(self, left_index, right_index) -> List[CharLegalAct]:
        chars = []
        index_of_first_char_in_tab = self.get_char_index(left_index)
        length_of_sequence = right_index - left_index + 1
        for current_index in range(index_of_first_char_in_tab, length_of_sequence + index_of_first_char_in_tab):
            chars.append(self.chars[current_index].char)
        return chars

    def get_chars_from_indexes_and_set_status(self, left_index, right_index, status: StatusOfText) -> List[
        CharTextSplit]:
        chars_with_status = self.get_chars_from_indexes_with_status(left_index, right_index)
        for char_with_status in chars_with_status:
            char_with_status.action_type = status
        return chars_with_status

    def get_chars_from_indexes_with_status(self, left_index, right_index) -> List[CharTextSplit]:
        chars = []
        index_of_first_char_in_tab = self.get_char_index(left_index)
        length_of_sequence = right_index - left_index + 1
        for current_index in range(index_of_first_char_in_tab, length_of_sequence + index_of_first_char_in_tab):
            chars.append(self.chars[current_index])
        return chars

    def get_char_index(self, index_in_legal_act):
        for char_index, split_char in enumerate(self.chars):
            if split_char.char.index_in_legal_act == index_in_legal_act:
                return char_index
        raise RuntimeError(f"Index is out of range in text split: {self.text}")

    def is_up_to_date(self):
        normalized_text = re.sub(r'[\s\n\r]+', '', self.text).strip()
        return normalized_text not in ["(uchylony)", "(uchylona)", "(pominięte)", "(pominięty)"]