from typing import Mapping, Any

from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.splits.point_split import PointSplit


class Subpoint(LegalUnit):

    def __init__(self, unit_id: str, text: str):
        super().__init__(unit_id, text)

    @classmethod
    def build(cls, point_split: PointSplit):
        subpoint_text = cls.get_current_text(point_split.split_item_for_further_processing())
        return Subpoint(
            unit_id=point_split.id_unit,
            text=subpoint_text
        )

    def to_dict(self):
        return {
            'unit_id': self.unit_id,
            'text': self.text,
            'type': str(Subpoint.__name__)
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return Subpoint(
            unit_id=dict_object['unit_id'],
            text=dict_object['text']
        )