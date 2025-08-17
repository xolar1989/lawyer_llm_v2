from abc import ABC
from dataclasses import dataclass, asdict
from typing import List

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds

@dataclass
class AbstractChunks(ABC):
    parts: List['Chunk']
    ELI: str
    invoke_id: str
    model_id: str


@dataclass(frozen=True)
class Chunk(MongodbObject):
    text: str
    build_by: List[LegalAnnotationIds]

    def __eq__(self, other):
        if not isinstance(other, Chunk):
            return False
        return self.text == other.text and self.build_by == other.build_by

    def __hash__(self):
        return hash((
            self.text,
            tuple(self.build_by),
        ))

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "build_by": [asdict(build_by) for build_by in self.build_by],
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'Chunk':
        return cls(
            text=d["text"],
            build_by=[LegalAnnotationIds(**x) for x in d.get("build_by", [])],
        )