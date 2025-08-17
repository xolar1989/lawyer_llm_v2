from dataclasses import dataclass, field
from typing import Protocol, Set, Tuple, Optional

from preprocessing.chunks.chunk import Chunk, AbstractChunks

@dataclass(frozen=True)
class RelevantLegalUnit:
    ELI: str
    unit_id: str
    section_id: Optional[str]
    subpoint_id: Optional[str]
    legal_unit: object = field(compare=False, hash=False)  # excluded from equality & hashing

    def get_ids(self):
        return self.unit_id, self.section_id, self.subpoint_id


class MatchingStrategy(Protocol):
    def match(
            self,
            chunks: AbstractChunks,
            exact_chunk: Chunk,
            legal_units_of_annotation: Set[Tuple[str, str, str | None, str | None]]
    ) -> bool:
        ...

    def get_legal_units_of_annotation(self, annotation: RelevantLegalUnit) -> Set[Tuple[str, str, str | None, str | None]]:
        ...
