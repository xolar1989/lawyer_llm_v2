from dataclasses import dataclass
from typing import List


@dataclass
class LegalAnnotationIds:
    article_ids: List[str]
    section_ids: List[str]
    subpoint_ids: List[str]

    def __eq__(self, other):
        if not isinstance(other, LegalAnnotationIds):
            return False
        return (
                self.article_ids == other.article_ids and
                self.section_ids == other.section_ids and
                self.subpoint_ids == other.subpoint_ids
        )

    def __hash__(self):
        return hash((
            tuple(self.article_ids),
            tuple(self.section_ids),
            tuple(self.subpoint_ids),
        ))
