from collections import namedtuple
from dataclasses import dataclass
from typing import List, Mapping, Any

from preprocessing.legal_acts.legal_act_document import LegalActDocument
from preprocessing.mongo_db.mongodb import MongodbObject


@dataclass
class LegalActTitleEmbeddingPair(MongodbObject):
    legal_act: LegalActDocument
    title_embedding: List[float]

    def to_dict(self):
        return {
            'legal_act': self.legal_act.to_dict(),
            'title_embedding': self.title_embedding
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            legal_act=LegalActDocument.from_dict(dict_object['legal_act']),
            title_embedding=dict_object['title_embedding']
        )


