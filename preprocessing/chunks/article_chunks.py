from dataclasses import dataclass
from typing import List, Mapping, Any

from preprocessing.chunks.chunk import Chunk, AbstractChunks
from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds


@dataclass
class ArticleChunks(AbstractChunks, MongodbObject):
    unit_id: str

    def to_dict(self) -> dict:
        return {
            "unit_id": self.unit_id,
            "parts": [part.to_dict() for part in self.parts],
            'ELI': self.ELI,
            'invoke_id': self.invoke_id,
            "model_id": self.model_id
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]) -> 'ArticleChunks':
        return cls(
            unit_id=dict_object["unit_id"],
            parts=[Chunk.from_dict(p) for p in dict_object.get("parts", [])],
            ELI=dict_object['ELI'],
            invoke_id=dict_object["invoke_id"],
            model_id=dict_object["model_id"]
        )





