import random
import re
from abc import ABC
from typing import List, Mapping, Any

from preprocessing.mongo_db.mongodb import MongodbObject

from preprocessing.pdf_structure.splits.text_split import TextSplit, StatusOfText


class LegalUnit(MongodbObject, ABC):

    def __init__(self, unit_id: str, text: str):
        self.unit_id = unit_id
        self.text = text

    def is_up_to_date(self):
        normalized_text = re.sub(r'[\s\n\r]+', '', self.text).strip()
        normalized_text = re.sub(r'[⁰¹²³⁴⁵⁶⁷⁸⁹]+\⁾\⁽?', '', normalized_text).strip()
        return normalized_text not in ["(uchylony)", "(uchylona)", "(uchylone)", "(pominięte)", "(pominięty)", '(utraciłmoc)']

    @staticmethod
    def is_ascending(legal_units_splits: List['LegalUnit']):
        ids = [unit.unit_id for unit in legal_units_splits]

        def extract_main_number(unit_id: str):
            match = re.match(r'^(\d+)', unit_id)
            return int(match.group(1)) if match else float('inf')

        # Build list of main numbers, preserving order
        main_numbers_in_order = [extract_main_number(id_) for id_ in ids]

        return main_numbers_in_order == sorted(main_numbers_in_order.copy())

    @classmethod
    def get_current_text(cls, text_split: TextSplit):
        return ''.join(char.char.text for char in text_split.chars if char.action_type in {StatusOfText.NOT_MATCH_CHANGE, StatusOfText.BEFORE_UPCOMING_CHANGE})

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        if dict_object['type'] == "Article":
            from preprocessing.pdf_structure.elements.article import Article
            return Article.from_dict(dict_object)
        elif dict_object['type'] == "Section":
            from preprocessing.pdf_structure.elements.section import Section
            return Section.from_dict(dict_object)
        elif dict_object['type'] == "Subpoint":
            from preprocessing.pdf_structure.elements.subpoint import Subpoint
            return Subpoint.from_dict(dict_object)
        return None