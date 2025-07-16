from typing import List, Mapping, Any

from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.pdf_structure.splits.section_split import SectionSplit


class Section(LegalUnit):

    def __init__(self, unit_id: str, text: str, subpoints: List[Subpoint]):
        super().__init__(unit_id, text)
        self.subpoints = subpoints

    @classmethod
    def build(cls, section_split: SectionSplit, art_id: str) -> 'Section':
        subpoint_list: List[Subpoint] = []
        text_split = section_split.split_item_for_further_processing()
        text = cls.get_current_text(text_split) if text_split else ''
        for subpoint_split in section_split.legal_units_indeed:
            if not subpoint_split.is_current_unit:
                continue
            subpoint = Subpoint.build(subpoint_split)
            if subpoint.is_up_to_date():
                subpoint_list.append(subpoint)
        if not cls.is_ascending(subpoint_list):
            raise RuntimeError(f"The ids of subpoints are not in ascending order {[item.unit_id for item in subpoint_list]},"
                               f"Article id: {art_id}"
                               f"Section id: {section_split.id_unit}")
        return Section(
            unit_id=section_split.id_unit,
            text=text,
            subpoints=subpoint_list
        )

    def to_dict(self):
        return {
            'unit_id':self.unit_id,
            'text': self.text,
            'subpoints': [subpoint.to_dict() for subpoint in self.subpoints],
            'type': str(Section.__name__)
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return Section(
            unit_id=dict_object['unit_id'],
            text=dict_object['text'],
            subpoints=[Subpoint.from_dict(subpoint_dict) for subpoint_dict in  dict_object['subpoints']]
        )
