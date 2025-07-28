from dataclasses import dataclass, asdict
from typing import List, Mapping, Any

from preprocessing.legal_acts.legal_act_document import LegalActDocument
from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds


@dataclass
class LegalUnitAnnotation(MongodbObject):
    legal_act_document: LegalActDocument
    units_ids: LegalAnnotationIds
    legal_units: List[LegalUnit]

    def to_dict(self):
        return {
            'legal_act_document': self.legal_act_document.to_dict(),
            'units_ids': asdict(self.units_ids),
            'legal_units': [legal_unit.to_dict() for legal_unit in self.legal_units]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            legal_act_document=LegalActDocument.from_dict(dict_object['legal_act_document']),
            units_ids=LegalAnnotationIds(**dict_object['units_ids']),
            legal_units=[LegalUnit.from_dict(legal_unit_dict) for legal_unit_dict in dict_object['legal_units']]
        )



