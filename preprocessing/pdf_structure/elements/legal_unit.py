from abc import ABC
from typing import List, Mapping, Any

from natsort import natsorted

from preprocessing.mongo_db.mongodb import MongodbObject

from preprocessing.pdf_structure.splits.text_split import TextSplit, StatusOfText


class LegalUnit(MongodbObject, ABC):

    def __init__(self, unit_id: str, text: str):
        self.unit_id = unit_id
        self.text = text

    @staticmethod
    def is_ascending(legal_units_splits: List['LegalUnit']):
        # Extract parts for sorting
        ids_of_splits = [legal_unit_split.unit_id for legal_unit_split in legal_units_splits]
        # Check if the list is sorted
        return ids_of_splits == natsorted(ids_of_splits)

    @classmethod
    def get_current_text(cls, text_split: TextSplit):
        return ''.join(char.char.text for char in text_split.chars if char.action_type in {StatusOfText.NOT_MATCH_CHANGE, StatusOfText.BEFORE_UPCOMING_CHANGE})

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        if dict_object['type'] == "Article":
            from article import Article
            return Article.from_dict(dict_object)
        elif dict_object['type'] == "Section":
            from section import Section
            return Section.from_dict(dict_object)
        elif dict_object['type'] == "Subpoint":
            from subpoint import Subpoint
            return Subpoint.from_dict(dict_object)
        return None